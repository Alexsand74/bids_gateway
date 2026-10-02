use chrono::Local;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone)]
pub struct Logger {
    inner: Arc<Mutex<LoggerInner>>,
}

struct LoggerInner {
    service_name: String,
    log_dir: PathBuf,
    current_file: Option<File>,
    current_bytes: u64,
    created_at: Instant,
    part_counter: usize,
    max_bytes: u64,
    max_age_secs: u64,
    retention_files: usize,
    min_rank: u8,
}

fn rank(level: &str) -> u8 {
    match level {
        "ERROR" => 0,
        "WARN" => 1,
        "INFO" => 2,
        _ => 3,
    }
}

impl Logger {
    pub fn init<P: AsRef<Path>>(
        service_name: &str,
        log_dir: P,
        min_level: &str,
        max_file_size_mb: u64,
        max_file_age_sec: u64,
        retention_files: usize,
    ) -> Self {
        let dir = log_dir.as_ref().to_path_buf();
        let _ = fs::create_dir_all(&dir);
        let mut inner = LoggerInner {
            service_name: service_name.to_string(),
            log_dir: dir,
            current_file: None,
            current_bytes: 0,
            created_at: Instant::now(),
            part_counter: 0,
            max_bytes: max_file_size_mb.saturating_mul(1024 * 1024).max(1),
            max_age_secs: max_file_age_sec.max(1),
            retention_files: retention_files.max(1),
            min_rank: rank(min_level),
        };
        inner.rotate();
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn debug(&self, msg: &str) {
        self.log("DEBUG", msg);
    }

    pub fn info(&self, msg: &str) {
        self.log("INFO", msg);
    }

    pub fn warn(&self, msg: &str) {
        self.log("WARN", msg);
    }

    pub fn error(&self, msg: &str) {
        self.log("ERROR", msg);
    }

    fn log(&self, level: &str, msg: &str) {
        let mut inner = match self.inner.lock() {
            Ok(v) => v,
            Err(_) => return,
        };
        if rank(level) > inner.min_rank {
            return;
        }
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let tid = std::thread::current().id();
        let line = format!(
            "{} | {:<5} | {} | tid={:?} | {}\n",
            timestamp, level, inner.service_name, tid, msg
        );
        print!("{}", line);
        inner.write_line(&line);
    }
}

impl LoggerInner {
    fn write_line(&mut self, line: &str) {
        let line_len = line.len() as u64;
        if self.current_file.is_none()
            || self.current_bytes + line_len >= self.max_bytes
            || self.created_at.elapsed().as_secs() >= self.max_age_secs
        {
            self.rotate();
        }
        if let Some(file) = &mut self.current_file {
            if file.write_all(line.as_bytes()).is_ok() {
                let _ = file.flush();
                self.current_bytes += line_len;
            }
        }
    }

    fn rotate(&mut self) {
        self.part_counter += 1;
        let stamp = Local::now().format("%Y-%m-%d_%H-%M-%S");
        let name = format!(
            "{}_{}_part{:03}.log",
            self.service_name, stamp, self.part_counter
        );
        let path = self.log_dir.join(name);
        if let Ok(file) = OpenOptions::new().create(true).append(true).open(path) {
            self.current_file = Some(file);
            self.current_bytes = 0;
            self.created_at = Instant::now();
        }
        self.apply_retention();
    }

    fn apply_retention(&self) {
        let Ok(entries) = fs::read_dir(&self.log_dir) else {
            return;
        };
        let mut logs: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| {
                        name.starts_with(&self.service_name) && name.ends_with(".log")
                    })
            })
            .collect();
        if logs.len() > self.retention_files {
            logs.sort();
            let extra = logs.len() - self.retention_files;
            for path in logs.into_iter().take(extra) {
                let _ = fs::remove_file(path);
            }
        }
    }
}
