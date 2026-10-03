//! Супервизор: слушатель входной очереди, пул воркеров с масштабированием,
//! watcher горячей перезагрузки каталога заявок.
//!
//! НОВОЕ: три действия во входящем запросе:
//!   "search_bids"     — поиск по ключевому слову (как раньше, но со статусом в ответе);
//!   "get_bid"         — полная карточка заявки по точному номеру;
//!   "get_bid_tovars"  — страница товарных позиций заявки (по 10 на страницу).

use crate::bus::{BusConsumer, BusProducer};
use crate::config::AppConfig;
use crate::logger::Logger;
use crate::models::{
    BidDetailsResponse, BidResultItem, BidTovarLineOut, BidTovarsResponse, HeapTask,
    SearchQueryRequest, SearchResponse,
};
use crate::normalizer::clean_text;
use crate::search::search_bids;
use crate::store::BidsStore;
use anyhow::Result;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Позиций товара на одну страницу в ответе "get_bid_tovars".
pub const TOVARS_PAGE_SIZE: usize = 10;

pub struct Supervisor {
    cfg: AppConfig,
    store: Arc<BidsStore>,
    logger: Logger,
    running: Arc<AtomicBool>,
    active_workers: Arc<AtomicUsize>,
    task_tx: Sender<HeapTask>,
    task_rx: Receiver<HeapTask>,
    producer: Arc<BusProducer>,
}

impl Supervisor {
    pub fn new(
        cfg: AppConfig,
        store: Arc<BidsStore>,
        logger: Logger,
        running: Arc<AtomicBool>,
    ) -> Result<Self> {
        let (task_tx, task_rx) = bounded::<HeapTask>(128);

        // Очередь ответов создаём сами (её будет читать Bids_Manager)
        let producer = Arc::new(BusProducer::connect_or_create(
            &cfg.queues.reply_queue,
            &running,
        )?);

        Ok(Self {
            cfg,
            store,
            logger,
            running,
            active_workers: Arc::new(AtomicUsize::new(0)),
            task_tx,
            task_rx,
            producer,
        })
    }

    pub fn start(self: &Arc<Self>) -> Result<()> {
        self.logger.info("[SYS] Запуск супервизора bids_gateway...");
        self.spawn_listener();
        self.spawn_watcher();
        self.spawn_pool_manager();

        let min = self.cfg.workers.min_threads.max(1);
        for _ in 0..min {
            self.spawn_worker();
        }
        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    // =========================================================
    // Слушатель входной очереди
    // =========================================================
    fn spawn_listener(self: &Arc<Self>) {
        let sup = Arc::clone(self);
        thread::spawn(move || {
            let q = sup.cfg.queues.request_queue.clone();
            sup.logger.info(&format!("[BUS] Подключение к входной очереди '{}'...", q));

            let consumer = loop {
                if !sup.running.load(Ordering::Relaxed) {
                    return;
                }
                match BusConsumer::connect(&q, &sup.running) {
                    Ok(c) => break c,
                    Err(e) => {
                        sup.logger.warn(&format!(
                            "[BUS] [WAIT] Очередь '{}' пока недоступна ({}), ждём 1 сек...",
                            q, e
                        ));
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            };

            sup.logger.info(&format!("[BUS] Слушатель очереди '{}' подключен!", q));

            let poll = sup.cfg.workers.poll_interval_ms as u32;
            while sup.running.load(Ordering::Relaxed) {
                match consumer.consume(poll) {
                    Ok(Some(task)) => {
                        let _ = sup.task_tx.send(task);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        sup.logger.error(&format!("[BUS] Ошибка consume: {}", e));
                        if sup.running.load(Ordering::Relaxed) {
                            thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            }
            sup.logger.info(&format!("[BUS] Слушатель очереди '{}' завершён.", q));
        });
    }

    // =========================================================
    // Watcher: горячая перезагрузка каталога при новых срезах
    // =========================================================
    fn spawn_watcher(self: &Arc<Self>) {
        let sup = Arc::clone(self);
        thread::spawn(move || {
            let interval_sec = sup.cfg.watcher.check_interval_sec;
            sup.logger.info(&format!(
                "[WATCHER] Запущен: проверка новых срезов каждые {} сек.",
                interval_sec
            ));

            loop {
                if !sup.running.load(Ordering::Relaxed) {
                    break;
                }

                let mut left = interval_sec;
                while left > 0 && sup.running.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_secs(1));
                    left -= 1;
                }
                if !sup.running.load(Ordering::Relaxed) {
                    break;
                }

                let before = sup.store.loaded_session();
                match sup.store.load_latest_valid_session(&sup.cfg, &sup.logger) {
                    Ok((session, count)) => {
                        if session != before {
                            sup.logger.info(&format!(
                                "[WATCHER] Новый срез заявок: сессия '{}', {} позиций (было: '{}')",
                                session, count, before
                            ));
                        }
                    }
                    Err(e) => {
                        sup.logger.warn(&format!("[WATCHER] Срез не обновлён: {:#}", e));
                    }
                }
            }
            sup.logger.info("[WATCHER] Завершён.");
        });
    }

    // =========================================================
    // Менеджер пула: масштабирование ВВЕРХ при росте очереди задач
    // =========================================================
    fn spawn_pool_manager(self: &Arc<Self>) {
        let sup = Arc::clone(self);
        thread::spawn(move || {
            loop {
                if !sup.running.load(Ordering::Relaxed) {
                    break;
                }
                let backlog = sup.task_rx.len();
                let active = sup.active_workers.load(Ordering::Relaxed);
                if backlog >= sup.cfg.workers.scale_up_backlog
                    && active < sup.cfg.workers.max_threads
                {
                    sup.logger.info(&format!(
                        "[POOL] Нагрузка: {} задач в канале при {} воркерах — добавляем воркера",
                        backlog, active
                    ));
                    sup.spawn_worker();
                }
                thread::sleep(Duration::from_millis(200));
            }
        });
    }

    // =========================================================
    // Воркер: берёт задачу из канала, ищет, публикует ответ
    // =========================================================
    fn spawn_worker(self: &Arc<Self>) {
        let sup = Arc::clone(self);
        let rx = sup.task_rx.clone();
        let idle_timeout = Duration::from_secs(sup.cfg.workers.idle_timeout_sec);
        let min_threads = sup.cfg.workers.min_threads.max(1);

        sup.active_workers.fetch_add(1, Ordering::Relaxed);
        sup.logger.info(&format!(
            "[POOL] Запущен воркер (активных: {})",
            sup.active_workers.load(Ordering::Relaxed)
        ));

        thread::spawn(move || {
            let mut last_activity = Instant::now();

            while sup.running.load(Ordering::Relaxed) {
                match rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(task) => {
                        last_activity = Instant::now();
                        sup.process_task(task);
                    }
                    Err(_) => {
                        let current = sup.active_workers.load(Ordering::Relaxed);
                        if current > min_threads && last_activity.elapsed() > idle_timeout {
                            sup.logger.info(&format!(
                                "[POOL] Воркер остановлен по таймауту простоя (осталось: {})",
                                sup.active_workers.load(Ordering::Relaxed)
                            ));
                            break;
                        }
                    }
                }
            }

            // Счётчик активных воркеров уменьшаем при ЛЮБОМ выходе из потока:
            // и по таймауту простоя, и при остановке сервиса. (В прежней версии
            // при выходе по idle счётчик не уменьшался — пул "замирал".)
            sup.active_workers.fetch_sub(1, Ordering::Relaxed);
        });
    }

    // =========================================================
    // Обработка одной задачи: маршрутизация по action
    // =========================================================
    fn process_task(self: &Arc<Self>, task: HeapTask) {
        let (action, keyword, field, number, page) =
            match serde_json::from_str::<SearchQueryRequest>(&task.payload_query) {
                Ok(req) => {
                    let p = req.params.unwrap_or_default();
                    (
                        req.action.unwrap_or_else(|| "search_bids".to_string()),
                        p.keyword,
                        p.field.unwrap_or_default(),
                        p.number.unwrap_or_default(),
                        p.page.unwrap_or(1).max(1),
                    )
                }
                // Пришёл не JSON — считаем это просто текстом поискового запроса
                Err(_) => (
                    "search_bids".to_string(),
                    task.payload_query.clone(),
                    String::new(),
                    String::new(),
                    1,
                ),
            };

        let (message_type, dump) = if action.eq_ignore_ascii_case("get_bid") {
            self.build_details(&number, &task)
        } else if action.eq_ignore_ascii_case("get_bid_tovars") {
            self.build_tovars(&number, page, &task)
        } else {
            self.build_search(&keyword, &field, &task)
        };

        if let Err(e) = self.producer.publish(
            "Bids_Gateway",
            &self.cfg.reply.target_service,
            &message_type,
            &task.user_id,
            "bids_gateway",
            "reply_bids",
            &dump,
            &task.message_id,
            &task.correlation_id,
        ) {
            self.logger.error(&format!("[BUS] Ошибка публикации ответа: {:#}", e));
        }
    }

    // =========================================================
    // Действие "search_bids": поиск + статус в каждой позиции
    // =========================================================
    fn build_search(&self, keyword: &str, field: &str, task: &HeapTask) -> (String, String) {
        let start = Instant::now();

        let response = if keyword.trim().len() < self.cfg.search.min_query_len {
            self.logger.warn(&format!(
                "[SEARCH] Слишком короткий запрос '{}' от {}",
                keyword.trim(),
                task.source_service
            ));
            SearchResponse {
                status: "error".to_string(),
                total: 0,
                truncated: false,
                result: Vec::new(),
            }
        } else {
            let snapshot = self.store.snapshot();
            let found = search_bids(&snapshot, keyword, field);
            let total = found.len();
            let truncated = total > self.cfg.search.max_items;

            let result: Vec<BidResultItem> = found
                .iter()
                .take(self.cfg.search.max_items)
                .map(|b| BidResultItem {
                    number: b.number.clone(),
                    date: b.date.clone(),
                    firm: b.firm.clone(),
                    firm2: b.firm2.clone(),
                    name: b.name.clone(),
                    comment: b.comment.clone(),
                    status: b.status.clone(),
                    items_count: b.items_count,
                    goods_preview: goods_preview(&b.goods),
                    month: b.month.clone(),
                })
                .collect();

            let field_label = if field.is_empty() { "any" } else { field };
            self.logger.info(&format!(
                "[SEARCH] Запрос '{}' (field={}) от {}: найдено {} поз. за {:?}",
                keyword, field_label, task.source_service, total, start.elapsed()
            ));

            SearchResponse {
                status: "ok".to_string(),
                total,
                truncated,
                result,
            }
        };

        (
            "bids.search.response".to_string(),
            serde_json::to_string(&response).unwrap_or_default(),
        )
    }

    // =========================================================
    // Действие "get_bid": полная карточка по точному номеру
    // =========================================================
    fn build_details(&self, number: &str, task: &HeapTask) -> (String, String) {
        let start = Instant::now();
        let needle = clean_text(number);

        let response = if needle.is_empty() {
            self.logger.warn(&format!(
                "[DETAILS] Пустой номер заявки от {}",
                task.source_service
            ));
            BidDetailsResponse {
                status: "error".to_string(),
                error: Some("Не указан номер заявки".to_string()),
                result: None,
            }
        } else {
            let snapshot = self.store.snapshot();
            match snapshot.iter().find(|b| b.number_norm == needle) {
                Some(b) => {
                    self.logger.info(&format!(
                        "[DETAILS] Заявка '{}' найдена за {:?}",
                        b.number,
                        start.elapsed()
                    ));
                    BidDetailsResponse {
                        status: "ok".to_string(),
                        error: None,
                        result: Some(b.to_details()),
                    }
                }
                None => {
                    self.logger.info(&format!("[DETAILS] Заявка '{}' не найдена", number));
                    BidDetailsResponse {
                        status: "error".to_string(),
                        error: Some(format!("Заявка '{}' не найдена", number)),
                        result: None,
                    }
                }
            }
        };

        (
            "bids.details.response".to_string(),
            serde_json::to_string(&response).unwrap_or_default(),
        )
    }

    // =========================================================
    // Действие "get_bid_tovars": страница товарных позиций
    // =========================================================
    fn build_tovars(&self, number: &str, page: usize, task: &HeapTask) -> (String, String) {
        let start = Instant::now();
        let needle = clean_text(number);

        let response = if needle.is_empty() {
            self.logger.warn(&format!(
                "[TOVARS] Пустой номер заявки от {}",
                task.source_service
            ));
            BidTovarsResponse {
                status: "error".to_string(),
                number: number.to_string(),
                error: Some("Не указан номер заявки".to_string()),
                total: 0,
                page: 1,
                pages: 0,
                result: Vec::new(),
            }
        } else {
            let snapshot = self.store.snapshot();
            match snapshot.iter().find(|b| b.number_norm == needle) {
                Some(b) => {
                    let total = b.tovar_lines.len();
                    let pages = if total == 0 {
                        0
                    } else {
                        (total + TOVARS_PAGE_SIZE - 1) / TOVARS_PAGE_SIZE
                    };
                    let page = if pages == 0 { 1 } else { page.clamp(1, pages) };
                    let start_idx = (page - 1) * TOVARS_PAGE_SIZE;
                    let end_idx = (start_idx + TOVARS_PAGE_SIZE).min(total);
                    let shown_from = if total == 0 { 0 } else { start_idx + 1 };

                    let result: Vec<BidTovarLineOut> = b.tovar_lines[start_idx..end_idx]
                        .iter()
                        .map(|l| BidTovarLineOut {
                            str_num: l.str_num,
                            name: l.name.clone(),
                            kolvo: l.kolvo.clone(),
                            edizm: l.edizm.clone(),
                        })
                        .collect();

                    self.logger.info(&format!(
                        "[TOVARS] Заявка '{}': позиции {}–{} из {} (стр. {}/{}) за {:?}",
                        b.number, shown_from, end_idx, total, page, pages, start.elapsed()
                    ));

                    BidTovarsResponse {
                        status: "ok".to_string(),
                        number: b.number.clone(),
                        error: None,
                        total,
                        page,
                        pages,
                        result,
                    }
                }
                None => {
                    self.logger.info(&format!("[TOVARS] Заявка '{}' не найдена", number));
                    BidTovarsResponse {
                        status: "error".to_string(),
                        number: number.to_string(),
                        error: Some(format!("Заявка '{}' не найдена", number)),
                        total: 0,
                        page: 1,
                        pages: 0,
                        result: Vec::new(),
                    }
                }
            }
        };

        (
            "bids.tovars.response".to_string(),
            serde_json::to_string(&response).unwrap_or_default(),
        )
    }
}

/// Короткий превью товаров заявки для показа пользователю (200 символов).
fn goods_preview(goods: &str) -> String {
    const MAX_CHARS: usize = 200;
    if goods.chars().count() <= MAX_CHARS {
        goods.to_string()
    } else {
        let cut: String = goods.chars().take(MAX_CHARS).collect();
        format!("{}…", cut)
    }
}
