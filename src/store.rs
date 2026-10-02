//! Хранилище заявок в памяти: загрузка срезов orekeeper + атомарная замена через ArcSwap.

use crate::config::AppConfig;
use crate::logger::Logger;
use crate::models::BidItem;
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
                old.len(),
                count
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
    /// (массив тоже поддерживается на всякий случай)
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
                let number = extract_field(record, &cfg.fields.number);
                let date = extract_field(record, &cfg.fields.date);
                let firm = extract_field(record, &cfg.fields.firm);
                let firm2 = extract_field(record, &cfg.fields.firm2);
                let name = extract_field(record, &cfg.fields.name);
                let comment = extract_field(record, &cfg.fields.comment);
                let (goods, items_count) = extract_goods(record, cfg);

                if number.is_empty()
                    && firm.is_empty()
                    && firm2.is_empty()
                    && name.is_empty()
                    && comment.is_empty()
                    && goods.is_empty()
                {
                    continue; // полностью пустая запись
                }

                items.push(build_bid_item(
                    number, date, firm, firm2, name, comment, goods, items_count,
                    &month, session_name,
                ));
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

/// Собирает все товарные позиции заявки в одну строку:
/// "Tovar; Tovar2; ..." — по ней ищем "по названию товара".
/// Tovars — словарь { "STT1": {...}, ... } или массив.
fn extract_goods(record: &serde_json::Value, cfg: &AppConfig) -> (String, usize) {
    let mut parts: Vec<String> = Vec::new();

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
            if !t1.is_empty() {
                parts.push(t1);
            }
            if !t2.is_empty() && t2 != t1 {
                parts.push(t2);
            }
        }
    }

    let count = parts.len();
    (parts.join("; "), count)
}

#[allow(clippy::too_many_arguments)]
fn build_bid_item(
    number: String,
    date: String,
    firm: String,
    firm2: String,
    name: String,
    comment: String,
    goods: String,
    items_count: usize,
    month: &str,
    session: &str,
) -> BidItem {
    let number_norm = clean_text(&number);
    let firm_norm = clean_text(&firm);
    let firm2_norm = clean_text(&firm2);
    let name_norm = clean_text(&name);
    let comment_norm = clean_text(&comment);
    let goods_norm = clean_text(&goods);

    let all_text = format!(
        "{} {} {} {} {} {}",
        number_norm, firm_norm, firm2_norm, name_norm, comment_norm, goods_norm
    );

    BidItem {
        number,
        date,
        firm,
        firm2,
        name,
        comment,
        goods,
        items_count,
        month: month.to_string(),
        session: session.to_string(),
        number_norm,
        firm_tokens: tokenize(&firm),
        firm2_tokens: tokenize(&firm2),
        name_tokens: tokenize(&name),
        comment_tokens: tokenize(&comment),
        goods_tokens: tokenize(&goods),
        all_text,
    }
}
