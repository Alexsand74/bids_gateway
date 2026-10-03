//! Хранилище заявок в памяти: загрузка срезов orekeeper + атомарная замена через ArcSwap.
//!
//! НОВОЕ: помимо прежних полей извлекаются Status, Srochnost, Sklad, Zakazal,
//! Manager, WishDate, PlanDate, CompDate, а товарные позиции сохраняются
//! в структурном виде (Vec<BidTovarLine>) для команды «Товары <номер>».

use crate::config::AppConfig;
use crate::logger::Logger;
use crate::models::{BidItem, BidTovarLine};
use crate::normalizer::{clean_text, tokenize};
use anyhow::{bail, Result};
use arc_swap::ArcSwap;
use serde::Deserialize;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct BidsStore {
    bids: ArcSwap<Vec<BidItem>>,
    loaded_session: Mutex<String>,
}

/// Сводный лог orekeeper по выгрузке заявок: <сессия>_bid_log.json
#[derive(Debug, Deserialize)]
struct OrekeeperBidLog {
    #[serde(default)]
    total_months: usize,
    #[serde(default)]
    success_months: usize,
    #[serde(default)]
    failed_months: usize,
}

/// Сырые поля одной заявки, извлечённые из JSON-записи.
struct RawBid {
    number: String,
    date: String,
    firm: String,
    firm2: String,
    name: String,
    comment: String,
    status: String,
    srochnost: String,
    sklad: String,
    zakazal: String,
    manager: String,
    wish_date: String,
    plan_date: String,
    comp_date: String,
}

impl BidsStore {
    pub fn new() -> Self {
        Self {
            bids: ArcSwap::from_pointee(Vec::new()),
            loaded_session: Mutex::new(String::new()),
        }
    }

    /// Текущий снимок каталога (читатели работают без локов).
    pub fn snapshot(&self) -> Arc<Vec<BidItem>> {
        self.bids.load_full()
    }

    pub fn loaded_session(&self) -> String {
        self.loaded_session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Ищет самый свежий валидный срез заявок, парсит его и загружает в RAM.
    pub fn load_latest_valid_session(&self, cfg: &AppConfig, log: &Logger) -> Result<(String, usize)> {
        let root = Path::new(&cfg.paths.bids_root);
        if !root.exists() {
            bail!("Папка выгрузок заявок не найдена: {}", root.display());
        }

        let mut sessions: Vec<PathBuf> = fs::read_dir(root)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        if sessions.is_empty() {
            bail!("В {} нет ни одной сессии выгрузки заявок", root.display());
        }

        // Имена сессий — таймстампы: лексикографическая сортировка = по времени.
        sessions.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

        for session_dir in sessions {
            let session_name = session_dir
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();

            if !self.is_session_valid(&session_dir, &session_name, cfg, log) {
                log.warn(&format!(
                    "[BIDS] Сессия '{}' повреждена (ошибки в логе orekeeper) — проверяем предыдущую...",
                    session_name
                ));
                continue;
            }

            let items = self.parse_session(&session_dir, &session_name, cfg, log)?;
            let count = items.len();

            let old = self.bids.load();
            log.info(&format!(
                "[BIDS] Замена каталога: старый вектор ({} поз.) -> новый вектор ({} поз.)",
                old.len(), count
            ));
            self.bids.store(Arc::new(items));
            drop(old);

            *self.loaded_session.lock().unwrap_or_else(|e| e.into_inner()) =
                session_name.clone();

            return Ok((session_name, count));
        }

        bail!("Не удалось найти ни одного валидного среза заявок");
    }

    /// Сессия валидна, если в логе orekeeper нет провальных месяцев.
    fn is_session_valid(&self, session_dir: &Path, session_name: &str, cfg: &AppConfig, log: &Logger) -> bool {
        let months_dir = session_dir.join("months");
        if !months_dir.exists() {
            log.warn(&format!("[BIDS] В сессии '{}' нет подпапки months", session_name));
            return false;
        }

        let logs_root = Path::new(&cfg.paths.logs_root);
        if !logs_root.exists() {
            return true;
        }

        if let Ok(entries) = fs::read_dir(logs_root) {
            for entry in entries.filter_map(|e| e.ok()) {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.contains(session_name) {
                    let Ok(file) = File::open(entry.path()) else {
                        return true;
                    };
                    if let Ok(report) =
                        serde_json::from_reader::<_, OrekeeperBidLog>(BufReader::new(file))
                    {
                        if report.failed_months > 0 || report.total_months == 0 {
                            log.error(&format!(
                                "[BIDS] В логе {:?} зафиксированы ошибки выгрузки заявок!",
                                entry.path()
                            ));
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// Разбор всех месяцев сессии: months\<ГГГГ-ММ>\supply_requests.json
    /// Формат файла: словарь { "ST1": {...заявка...}, "ST2": {...} }
    fn parse_session(
        &self,
        session_dir: &Path,
        session_name: &str,
        cfg: &AppConfig,
        log: &Logger,
    ) -> Result<Vec<BidItem>> {
        let months_root = session_dir.join("months");
        let mut month_dirs: Vec<PathBuf> = fs::read_dir(&months_root)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        // Свежие месяцы первыми
        month_dirs.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

        let mut items = Vec::new();
        for month_dir in month_dirs {
            let month = month_dir
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            let file_path = month_dir.join("supply_requests.json");
            if !file_path.exists() {
                continue;
            }

            let Ok(file) = File::open(&file_path) else { continue };
            let Ok(value) = serde_json::from_reader::<_, serde_json::Value>(BufReader::new(file))
            else {
                log.warn(&format!("[BIDS] Не удалось разобрать {}", file_path.display()));
                continue;
            };

            let records: Box<dyn Iterator<Item = &serde_json::Value>> =
                if let Some(arr) = value.as_array() {
                    Box::new(arr.iter())
                } else if let Some(obj) = value.as_object() {
                    Box::new(obj.values())
                } else {
                    continue;
                };

            let mut loaded = 0usize;
            for record in records {
                let raw = RawBid {
                    number: extract_field(record, &cfg.fields.number),
                    date: extract_field(record, &cfg.fields.date),
                    firm: extract_field(record, &cfg.fields.firm),
                    firm2: extract_field(record, &cfg.fields.firm2),
                    name: extract_field(record, &cfg.fields.name),
                    comment: extract_field(record, &cfg.fields.comment),
                    status: extract_field(record, &cfg.fields.status),
                    srochnost: extract_field(record, &cfg.fields.srochnost),
                    sklad: extract_field(record, &cfg.fields.sklad),
                    zakazal: extract_field(record, &cfg.fields.zakazal),
                    manager: extract_field(record, &cfg.fields.manager),
                    wish_date: extract_field(record, &cfg.fields.wish_date),
                    plan_date: extract_field(record, &cfg.fields.plan_date),
                    comp_date: extract_field(record, &cfg.fields.comp_date),
                };
                let (goods, tovar_lines) = extract_tovars(record, cfg);

                if raw.number.is_empty()
                    && raw.firm.is_empty()
                    && raw.firm2.is_empty()
                    && raw.name.is_empty()
                    && raw.comment.is_empty()
                    && goods.is_empty()
                {
                    continue; // полностью пустая запись
                }

                items.push(build_bid_item(raw, goods, tovar_lines, &month, session_name));
                loaded += 1;
            }
            log.info(&format!("[BIDS] Месяц {}: загружено {} заявок", month, loaded));
        }
        Ok(items)
    }
}

/// Достаёт строковое поле из JSON-записи по настраиваемому имени.
/// Понимает строки, числа и вложенные объекты вида {"value": "..."}.
fn extract_field(record: &serde_json::Value, field_name: &str) -> String {
    if field_name.is_empty() {
        return String::new();
    }
    match record.get(field_name) {
        Some(serde_json::Value::String(s)) => s.trim().to_string(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Object(o)) => o
            .get("value")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

/// Собирает товарные позиции заявки. Возвращает:
/// 1) строку "Tovar; Tovar2; ..." — по ней идёт полнотекстовый поиск по товарам
///    (поведение прежней версии сохранено: при различии имён в строку попадают оба);
/// 2) структурные строки для показа: name = Tovar (полное из номенклатуры),
///    если не пуст; иначе Tovar2. items_count теперь равен числу записей Tovars.
fn extract_tovars(record: &serde_json::Value, cfg: &AppConfig) -> (String, Vec<BidTovarLine>) {
    let mut parts: Vec<String> = Vec::new();
    let mut lines: Vec<BidTovarLine> = Vec::new();

    if let Some(tovars) = record.get(&cfg.fields.tovars) {
        let items: Box<dyn Iterator<Item = &serde_json::Value>> = if let Some(arr) = tovars.as_array()
        {
            Box::new(arr.iter())
        } else if let Some(obj) = tovars.as_object() {
            Box::new(obj.values())
        } else {
            Box::new(std::iter::empty())
        };

        for item in items {
            let t1 = extract_field(item, &cfg.fields.tovar);
            let t2 = extract_field(item, &cfg.fields.tovar2);

            // Сравнение ДО перемещения в parts (иначе borrow of moved value)
            if !t1.is_empty() && t2 != t1 {
                parts.push(t1.clone());
            }
            if !t2.is_empty() {
                parts.push(t2.clone());
            }

            // Для показа: полное название из номенклатуры, иначе короткое.
            let display = if !t1.is_empty() { t1 } else { t2 };
            if display.is_empty() {
                continue;
            }

            let str_num: u32 = extract_field(item, &cfg.fields.str_num)
                .parse()
                .unwrap_or(lines.len() as u32 + 1);
            let kolvo = extract_field(item, &cfg.fields.kolvo);
            let edizm = extract_field(item, &cfg.fields.edizm);

            lines.push(BidTovarLine {
                str_num,
                name: display,
                kolvo,
                edizm,
            });
        }
    }

    // Гарантируем порядок позиций как в 1С (STT1, STT2, ... STT117)
    lines.sort_by_key(|l| l.str_num);
    (parts.join("; "), lines)
}

/// Сначала считаем все производные значения (токены), ПОТОМ двигаем строки
/// в структуру — иначе borrow of moved value.
fn build_bid_item(
    raw: RawBid,
    goods: String,
    tovar_lines: Vec<BidTovarLine>,
    month: &str,
    session: &str,
) -> BidItem {
    let number_norm = clean_text(&raw.number);
    let firm_tokens = tokenize(&raw.firm);
    let firm2_tokens = tokenize(&raw.firm2);
    let name_tokens = tokenize(&raw.name);
    let comment_tokens = tokenize(&raw.comment);
    let goods_tokens = tokenize(&goods);

    let all_text = format!(
        "{} {} {} {} {} {}",
        number_norm,
        clean_text(&raw.firm),
        clean_text(&raw.firm2),
        clean_text(&raw.name),
        clean_text(&raw.comment),
        clean_text(&goods)
    );

    let items_count = tovar_lines.len();

    BidItem {
        number: raw.number,
        date: raw.date,
        firm: raw.firm,
        firm2: raw.firm2,
        name: raw.name,
        comment: raw.comment,
        status: raw.status,
        srochnost: raw.srochnost,
        sklad: raw.sklad,
        zakazal: raw.zakazal,
        manager: raw.manager,
        wish_date: raw.wish_date,
        plan_date: raw.plan_date,
        comp_date: raw.comp_date,
        goods,
        items_count,
        tovar_lines,
        month: month.to_string(),
        session: session.to_string(),
        number_norm,
        firm_tokens,
        firm2_tokens,
        name_tokens,
        comment_tokens,
        goods_tokens,
        all_text,
    }
}
