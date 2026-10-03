mod bus;
mod config;
mod logger;
mod models;
mod normalizer;
mod search;
mod store;
mod supervisor;

use anyhow::Result;
use config::AppConfig;
use logger::Logger;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use store::BidsStore;
use supervisor::Supervisor;

fn main() -> Result<()> {
    let cfg = AppConfig::load_from_file("config.json")?;
    let log = Logger::init(
        "Bids_Gateway",
        &cfg.logging.log_dir,
        &cfg.logging.min_level,
        cfg.logging.max_file_size_mb,
        cfg.logging.max_file_age_sec,
        cfg.logging.retention_max_files,
    );

    log.info("========================================================");
    log.info("   BIDS_GATEWAY — поиск заявок по выгрузкам orekeeper    ");
    log.info("========================================================");
    log.info(&format!("[ CFG ] Выгрузки заявок : {}", cfg.paths.bids_root));
    log.info(&format!("[ CFG ] Логи orekeeper  : {}", cfg.paths.logs_root));
    log.info(&format!("[ IN  ] Request queue   : {}", cfg.queues.request_queue));
    log.info(&format!("[ OUT ] Reply queue     : {}", cfg.queues.reply_queue));
    log.info(&format!("[ OUT ] Target service  : {}", cfg.reply.target_service));
    log.info(&format!(
        "[ CFG ] Воркеры         : {}..{} (backlog {} -> рост)",
        cfg.workers.min_threads, cfg.workers.max_threads, cfg.workers.scale_up_backlog
    ));
    log.info(&format!("[ CFG ] Watcher         : каждые {} сек", cfg.watcher.check_interval_sec));
    log.info(&format!(
        "[ CFG ] Поля заявок     : номер='{}' дата='{}' статус='{}' срочность='{}' склад='{}' заказал='{}' менеджер='{}' даты='{}/'{}'/'{}' орг='{}'/'{}' наименование='{}' комментарий='{}' товары='{}'",
        cfg.fields.number, cfg.fields.date, cfg.fields.status, cfg.fields.srochnost,
        cfg.fields.sklad, cfg.fields.zakazal, cfg.fields.manager,
        cfg.fields.wish_date, cfg.fields.plan_date, cfg.fields.comp_date,
        cfg.fields.firm, cfg.fields.firm2,
        cfg.fields.name, cfg.fields.comment, cfg.fields.tovars
    ));
    log.info(&format!(
        "[ CFG ] Поля товаров    : строка='{}' товар='{}'/'{}' кол-во='{}' ед.изм.='{}'",
        cfg.fields.str_num, cfg.fields.tovar, cfg.fields.tovar2,
        cfg.fields.kolvo, cfg.fields.edizm
    ));
    log.info("[ CFG ] Действия        : search_bids | get_bid | get_bid_tovars");

    // Ctrl+C -> graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        let l = log.clone();
        ctrlc::set_handler(move || {
            l.info("[SYS] Получен сигнал остановки (Ctrl+C). Завершаем Bids_Gateway...");
            r.store(false, Ordering::SeqCst);
        })
            .expect("Не удалось установить Ctrl+C handler");
    }

    // Начальная загрузка каталога заявок
    let store = Arc::new(BidsStore::new());
    match store.load_latest_valid_session(&cfg, &log) {
        Ok((session, count)) => log.info(&format!(
            "Начальный каталог заявок загружен: сессия '{}', {} заявок в памяти",
            session, count
        )),
        Err(e) => log.warn(&format!(
            "Заявки пока не загружены ({}). Watcher повторит попытку.",
            e
        )),
    }

    // Самотест: отправить один запрос в собственную входную очередь
    if cfg.self_test.enabled {
        let st_cfg = cfg.clone();
        let st_running = running.clone();
        let st_log = log.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(3));
            if !st_running.load(Ordering::Relaxed) {
                return;
            }
            let producer = match bus::BusProducer::connect_or_create(
                &st_cfg.queues.request_queue,
                &st_running,
            ) {
                Ok(p) => p,
                Err(e) => {
                    st_log.warn(&format!("[SELF-TEST] Не удалось создать очередь: {:#}", e));
                    return;
                }
            };

            let body = serde_json::json!({
                "task_id": "self-test",
                "source": "self_test",
                "action": "search_bids",
                "params": {
                    "keyword": st_cfg.self_test.keyword,
                    "field": st_cfg.self_test.field
                }
            });
            match producer.publish(
                "Self_Test",
                "Bids_Gateway",
                "bids.search.request",
                "0",
                "self_test",
                "search_bids",
                &body.to_string(),
                "",
                "",
            ) {
                Ok(()) => st_log.info("[SELF-TEST] Тестовый запрос отправлен во входную очередь."),
                Err(e) => st_log.warn(&format!(
                    "[SELF-TEST] Не удалось отправить тестовый запрос: {:#}",
                    e
                )),
            }

            // ВАЖНО: держим producer живым до остановки сервиса!
            // Shared memory живёт, пока открыт хотя бы один хендл.
            // Если поток завершится сразу после publish, очередь вместе
            // с сообщением уничтожится раньше, чем слушатель (он проверяет
            // раз в секунду) успеет к ней подключиться.
            while st_running.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
            }
            drop(producer);
        });
    }

    let supervisor = Arc::new(Supervisor::new(cfg, store, log.clone(), running.clone())?);
    supervisor.start()?;

    while running.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(200));
    }

    supervisor.stop();
    log.info("[SYS] Bids_Gateway остановлен корректно.");
    Ok(())
}
