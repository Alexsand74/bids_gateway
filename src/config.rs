//! Парсер config.json. Все поля со `serde(default)`, после чтения — sanity-клампы.
//! Пути ОТНОСИТЕЛЬНЫЕ (как у stock_gateway): сервис запускается из своей папки.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

fn d_request_queue() -> String { "BidsManager_To_BidsGateway".into() }
fn d_reply_queue() -> String { "BidsGateway_To_BidsManager".into() }
fn d_bids_root() -> String { "../orekeeper/data/bid".into() }
fn d_logs_root() -> String { "../orekeeper/logs/bid".into() }
fn d_target() -> String { "Bids_Manager".into() }
// Реальные имена ключей в supply_requests.json (проверено на выгрузке orekeeper)
fn d_f_number() -> String { "Number".into() }
fn d_f_date() -> String { "Date".into() }
fn d_f_firm() -> String { "ZakazPodr".into() }
fn d_f_firm2() -> String { "ZakupPodr".into() }
fn d_f_name() -> String { "Name".into() }
fn d_f_comment() -> String { "Details".into() }
fn d_f_tovars() -> String { "Tovars".into() }
fn d_f_tovar() -> String { "Tovar".into() }
fn d_f_tovar2() -> String { "Tovar2".into() }
fn d_max_items() -> usize { 50 }
fn d_min_query_len() -> usize { 2 }
fn d_min_threads() -> usize { 2 }
fn d_max_threads() -> usize { 8 }
fn d_poll_ms() -> u64 { 50 }
fn d_idle_sec() -> u64 { 30 }
fn d_backlog() -> usize { 16 }
fn d_check_sec() -> u64 { 1800 }
fn d_st_enabled() -> bool { false }
fn d_st_keyword() -> String { "стрейч".into() }
fn d_st_field() -> String { "any".into() }
fn d_log_dir() -> String { "logs".into() }
fn d_min_level() -> String { "INFO".into() }
fn d_mb() -> u64 { 5 }
fn d_age() -> u64 { 3600 }
fn d_ret() -> usize { 100 }

#[derive(Debug, Clone, Deserialize)]
pub struct Queues {
    #[serde(default = "d_request_queue")] pub request_queue: String,
    #[serde(default = "d_reply_queue")] pub reply_queue: String,
}
impl Default for Queues {
    fn default() -> Self {
        Self { request_queue: d_request_queue(), reply_queue: d_reply_queue() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Paths {
    #[serde(default = "d_bids_root")] pub bids_root: String,
    #[serde(default = "d_logs_root")] pub logs_root: String,
}
impl Default for Paths {
    fn default() -> Self {
        Self { bids_root: d_bids_root(), logs_root: d_logs_root() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Reply {
    #[serde(default = "d_target")] pub target_service: String,
}
impl Default for Reply {
    fn default() -> Self { Self { target_service: d_target() } }
}

/// Имена ключей JSON внутри supply_requests.json.
#[derive(Debug, Clone, Deserialize)]
pub struct Fields {
    pub number: String,   // номер заявки ("ЦЕХ_М-00546")
    pub date: String,     // дата
    pub firm: String,    // ZakazPodr — заказывающее подразделение (организация)
    pub firm2: String,   // ZakupPodr — закупающее подразделение
    pub name: String,    // название заявки
    pub comment: String, // комментарий (Details)
    pub tovars: String,  // словарь позиций товаров
    pub tovar: String,   // название позиции (полное)
    pub tovar2: String,  // название позиции (короткое)
}
impl Default for Fields {
    fn default() -> Self {
        Self {
            number: d_f_number(),
            date: d_f_date(),
            firm: d_f_firm(),
            firm2: d_f_firm2(),
            name: d_f_name(),
            comment: d_f_comment(),
            tovars: d_f_tovars(),
            tovar: d_f_tovar(),
            tovar2: d_f_tovar2(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Search {
    #[serde(default = "d_max_items")] pub max_items: usize,
    #[serde(default = "d_min_query_len")] pub min_query_len: usize,
}
impl Default for Search {
    fn default() -> Self {
        Self { max_items: d_max_items(), min_query_len: d_min_query_len() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Workers {
    #[serde(default = "d_min_threads")] pub min_threads: usize,
    #[serde(default = "d_max_threads")] pub max_threads: usize,
    #[serde(default = "d_poll_ms")] pub poll_interval_ms: u64,
    #[serde(default = "d_idle_sec")] pub idle_timeout_sec: u64,
    #[serde(default = "d_backlog")] pub scale_up_backlog: usize,
}
impl Default for Workers {
    fn default() -> Self {
        Self {
            min_threads: d_min_threads(),
            max_threads: d_max_threads(),
            poll_interval_ms: d_poll_ms(),
            idle_timeout_sec: d_idle_sec(),
            scale_up_backlog: d_backlog(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Watcher {
    #[serde(default = "d_check_sec")] pub check_interval_sec: u64,
}
impl Default for Watcher {
    fn default() -> Self { Self { check_interval_sec: d_check_sec() } }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SelfTestCfg {
    #[serde(default = "d_st_enabled")] pub enabled: bool,
    #[serde(default = "d_st_keyword")] pub keyword: String,
    #[serde(default = "d_st_field")] pub field: String,
}
impl Default for SelfTestCfg {
    fn default() -> Self {
        Self { enabled: d_st_enabled(), keyword: d_st_keyword(), field: d_st_field() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Logging {
    #[serde(default = "d_log_dir")] pub log_dir: String,
    #[serde(default = "d_min_level")] pub min_level: String,
    #[serde(default = "d_mb")] pub max_file_size_mb: u64,
    #[serde(default = "d_age")] pub max_file_age_sec: u64,
    #[serde(default = "d_ret")] pub retention_max_files: usize,
}
impl Default for Logging {
    fn default() -> Self {
        Self {
            log_dir: d_log_dir(),
            min_level: d_min_level(),
            max_file_size_mb: d_mb(),
            max_file_age_sec: d_age(),
            retention_max_files: d_ret(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)] pub queues: Queues,
    #[serde(default)] pub paths: Paths,
    #[serde(default)] pub reply: Reply,
    #[serde(default)] pub fields: Fields,
    #[serde(default)] pub search: Search,
    #[serde(default)] pub workers: Workers,
    #[serde(default)] pub watcher: Watcher,
    #[serde(default)] pub self_test: SelfTestCfg,
    #[serde(default)] pub logging: Logging,
}

impl AppConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let raw = match File::open(path.as_ref()) {
            Ok(file) => {
                let reader = BufReader::new(file);
                serde_json::from_reader::<_, AppConfig>(reader)
                    .with_context(|| format!("Ошибка разбора {:?}", path.as_ref()))?
            }
            Err(_) => {
                eprintln!(
                    "[CONFIG] Файл {:?} не найден — используются значения по умолчанию.",
                    path.as_ref()
                );
                AppConfig::default()
            }
        };

        let mut cfg = raw;
        cfg.workers.min_threads = cfg.workers.min_threads.max(1);
        cfg.workers.max_threads = cfg.workers.max_threads.max(cfg.workers.min_threads);
        cfg.workers.poll_interval_ms = cfg.workers.poll_interval_ms.max(1);
        cfg.workers.idle_timeout_sec = cfg.workers.idle_timeout_sec.max(1);
        cfg.workers.scale_up_backlog = cfg.workers.scale_up_backlog.max(1);
        cfg.watcher.check_interval_sec = cfg.watcher.check_interval_sec.max(10);
        cfg.search.max_items = cfg.search.max_items.max(1);
        cfg.search.min_query_len = cfg.search.min_query_len.max(1);
        cfg.logging.max_file_size_mb = cfg.logging.max_file_size_mb.max(1);
        cfg.logging.max_file_age_sec = cfg.logging.max_file_age_sec.max(10);
        cfg.logging.retention_max_files = cfg.logging.retention_max_files.max(2);

        Ok(cfg)
    }
}
