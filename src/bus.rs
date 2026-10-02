//! SHM-шина на raw FFI (kernel32), без крейта `windows`.
//! База — вариант из ai_manager: прерываемый по Ctrl+C connect (флаг running),
//! обработка WAIT_ABANDONED, аккуратное закрытие хендлов.
//! Протокол полностью совместим с остальными сервисами: Global\KK_<очередь>_...

use crate::models::*;
use anyhow::{anyhow, Result};
use std::os::raw::{c_char, c_void};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HANDLE = *mut c_void;
type BOOL = i32;
type DWORD = u32;
type LONG = i32;

const WAIT_OBJECT_0: DWORD = 0x0;
const WAIT_ABANDONED: DWORD = 0x80;
const WAIT_TIMEOUT: DWORD = 0x102;
const INFINITE: DWORD = 0xFFFF_FFFF;
const FILE_MAP_ALL_ACCESS: DWORD = 0x000F_001F;
const PAGE_READWRITE: DWORD = 0x04;
const SYNCHRONIZE: DWORD = 0x0010_0000;
const SEMAPHORE_ALL_ACCESS: DWORD = 0x001F_0003;
const ERROR_ALREADY_EXISTS: DWORD = 183;

#[link(name = "kernel32")]
unsafe extern "system" {
    fn OpenFileMappingA(dwDesiredAccess: DWORD, bInheritHandle: BOOL, lpName: *const c_char) -> HANDLE;
    fn CreateFileMappingA(hFile: HANDLE, sa: *mut c_void, flProtect: DWORD,
                          szHigh: DWORD, szLow: DWORD, lpName: *const c_char) -> HANDLE;
    fn MapViewOfFile(h: HANDLE, access: DWORD, offHigh: DWORD, offLow: DWORD, len: usize) -> *mut c_void;
    fn UnmapViewOfFile(base: *const c_void) -> BOOL;
    fn CloseHandle(h: HANDLE) -> BOOL;
    fn OpenMutexA(access: DWORD, inherit: BOOL, name: *const c_char) -> HANDLE;
    fn CreateMutexA(sa: *mut c_void, initial_owner: BOOL, name: *const c_char) -> HANDLE;
    fn ReleaseMutex(h: HANDLE) -> BOOL;
    fn OpenSemaphoreA(access: DWORD, inherit: BOOL, name: *const c_char) -> HANDLE;
    fn CreateSemaphoreA(sa: *mut c_void, initial: LONG, max: LONG, name: *const c_char) -> HANDLE;
    fn ReleaseSemaphore(h: HANDLE, count: LONG, prev: *mut LONG) -> BOOL;
    fn WaitForSingleObject(h: HANDLE, ms: DWORD) -> DWORD;
    fn GetLastError() -> DWORD;
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn build_id(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{:04}", prefix, current_time_ms(), count)
}

fn cname(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

fn wait_acquired(r: DWORD) -> bool {
    r == WAIT_OBJECT_0 || r == WAIT_ABANDONED
}

/// Спим total_ms кусками по 100 мс — быстрая реакция на Ctrl+C.
fn sleep_interruptible(total_ms: u64, running: &AtomicBool) -> bool {
    let mut left = total_ms;
    while left > 0 {
        if !running.load(Ordering::SeqCst) {
            return false;
        }
        let step = 100u64.min(left);
        std::thread::sleep(Duration::from_millis(step));
        left -= step;
    }
    running.load(Ordering::SeqCst)
}

fn safe_copy_bytes(dest: &mut [u8], src: &str) {
    let bytes = src.as_bytes();
    let len = bytes.len().min(dest.len().saturating_sub(1));
    dest[..len].copy_from_slice(&bytes[..len]);
    dest[len] = 0;
}

fn bytes_to_string(bytes: &[u8]) -> String {
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..len]).to_string()
}

fn task_from_slot(raw: &BusMessage) -> HeapTask {
    HeapTask {
        source_service: bytes_to_string(&raw.metadata.source_service),
        target_service: bytes_to_string(&raw.metadata.target_service),
        message_type: bytes_to_string(&raw.metadata.message_type),
        user_id: bytes_to_string(&raw.metadata.user_id),
        correlation_id: bytes_to_string(&raw.metadata.correlation_id),
        causation_id: bytes_to_string(&raw.metadata.causation_id),
        message_id: bytes_to_string(&raw.metadata.message_id),
        source: bytes_to_string(&raw.payload.source),
        task_type: bytes_to_string(&raw.payload.task_type),
        payload_query: bytes_to_string(&raw.payload.query),
    }
}

// ============================================================
//  BusConsumer
// ============================================================
pub struct BusConsumer {
    queue_name: String,
    h_map: HANDLE,
    h_mutex: HANDLE,
    h_items: HANDLE,
    h_spaces: HANDLE,
    ptr: *mut BusSharedQueue,
}

unsafe impl Send for BusConsumer {}

impl BusConsumer {
    /// Подключение к существующей очереди. `running` — общий флаг остановки:
    /// если его сбросили во время ожидания очереди — выходим с ошибкой.
    pub fn connect(queue_name: &str, running: &AtomicBool) -> Result<Self> {
        let base = |sfx: &str| format!("Global\\KK_{}_{}", queue_name, sfx);
        let shm_n = cname(&base("SharedMemory"));
        let mut_n = cname(&base("Mutex"));
        let itm_n = cname(&base("Items"));
        let spc_n = cname(&base("Spaces"));

        unsafe {
            let mut h_map: HANDLE = ptr::null_mut();
            for _ in 0..600 {
                h_map = OpenFileMappingA(FILE_MAP_ALL_ACCESS, 0, shm_n.as_ptr() as *const c_char);
                if !h_map.is_null() {
                    break;
                }
                println!("[BUS] [WAIT] queue='{}' жду shared memory...", queue_name);
                if !sleep_interruptible(1000, running) {
                    return Err(anyhow!("shutdown во время ожидания очереди {}", queue_name));
                }
            }
            if h_map.is_null() {
                return Err(anyhow!("OpenFileMappingA failed for {}", queue_name));
            }

            let raw = MapViewOfFile(h_map, FILE_MAP_ALL_ACCESS, 0, 0,
                                    std::mem::size_of::<BusSharedQueue>());
            if raw.is_null() {
                CloseHandle(h_map);
                return Err(anyhow!("MapViewOfFile failed for {}", queue_name));
            }
            let ptr = raw as *mut BusSharedQueue;

            let mut h_mutex: HANDLE = ptr::null_mut();
            for _ in 0..1200 {
                h_mutex = OpenMutexA(SYNCHRONIZE, 0, mut_n.as_ptr() as *const c_char);
                if !h_mutex.is_null() {
                    break;
                }
                if !sleep_interruptible(500, running) {
                    UnmapViewOfFile(raw);
                    CloseHandle(h_map);
                    return Err(anyhow!("shutdown во время ожидания mutex {}", queue_name));
                }
            }
            if h_mutex.is_null() {
                UnmapViewOfFile(raw);
                CloseHandle(h_map);
                return Err(anyhow!("OpenMutexA failed for {}", queue_name));
            }

            let mut h_items: HANDLE = ptr::null_mut();
            for _ in 0..1200 {
                h_items = OpenSemaphoreA(SEMAPHORE_ALL_ACCESS, 0, itm_n.as_ptr() as *const c_char);
                if !h_items.is_null() {
                    break;
                }
                if !sleep_interruptible(500, running) {
                    CloseHandle(h_mutex);
                    UnmapViewOfFile(raw);
                    CloseHandle(h_map);
                    return Err(anyhow!("shutdown во время ожидания items {}", queue_name));
                }
            }
            if h_items.is_null() {
                CloseHandle(h_mutex);
                UnmapViewOfFile(raw);
                CloseHandle(h_map);
                return Err(anyhow!("OpenSemaphoreA(items) failed for {}", queue_name));
            }

            let mut h_spaces: HANDLE = ptr::null_mut();
            for _ in 0..1200 {
                h_spaces = OpenSemaphoreA(SEMAPHORE_ALL_ACCESS, 0, spc_n.as_ptr() as *const c_char);
                if !h_spaces.is_null() {
                    break;
                }
                if !sleep_interruptible(500, running) {
                    CloseHandle(h_items);
                    CloseHandle(h_mutex);
                    UnmapViewOfFile(raw);
                    CloseHandle(h_map);
                    return Err(anyhow!("shutdown во время ожидания spaces {}", queue_name));
                }
            }
            if h_spaces.is_null() {
                CloseHandle(h_items);
                CloseHandle(h_mutex);
                UnmapViewOfFile(raw);
                CloseHandle(h_map);
                return Err(anyhow!("OpenSemaphoreA(spaces) failed for {}", queue_name));
            }

            println!("[BUS] [CONNECT] Consumer подключен к очереди '{}'.", queue_name);
            Ok(Self {
                queue_name: queue_name.to_string(),
                h_map,
                h_mutex,
                h_items,
                h_spaces,
                ptr,
            })
        }
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn consume(&self, timeout_ms: u32) -> Result<Option<HeapTask>> {
        unsafe {
            let r = WaitForSingleObject(self.h_items, timeout_ms);
            if r == WAIT_TIMEOUT {
                return Ok(None);
            }
            if r != WAIT_OBJECT_0 {
                return Err(anyhow!("WaitForSingleObject(items) failed: {}", r));
            }

            let rm = WaitForSingleObject(self.h_mutex, INFINITE);
            if !wait_acquired(rm) {
                ReleaseSemaphore(self.h_items, 1, ptr::null_mut());
                return Err(anyhow!("WaitForSingleObject(mutex) failed: {}", rm));
            }

            let q = &mut *self.ptr;
            let cap = if q.capacity > 0 { q.capacity } else { BUS_QUEUE_CAPACITY as i32 };
            let head = q.head;
            let idx = (head % cap) as usize;

            let task = task_from_slot(&q.messages[idx]);

            // Чистим слот в общей памяти (512 КБ на сообщение)
            std::ptr::write_bytes(
                &mut q.messages[idx] as *mut BusMessage as *mut u8,
                0,
                std::mem::size_of::<BusMessage>(),
            );
            q.head = (head + 1) % cap;

            ReleaseMutex(self.h_mutex);
            ReleaseSemaphore(self.h_spaces, 1, ptr::null_mut());
            Ok(Some(task))
        }
    }
}

impl Drop for BusConsumer {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                UnmapViewOfFile(self.ptr as *const c_void);
            }
            if !self.h_spaces.is_null() { CloseHandle(self.h_spaces); }
            if !self.h_items.is_null() { CloseHandle(self.h_items); }
            if !self.h_mutex.is_null() { CloseHandle(self.h_mutex); }
            if !self.h_map.is_null() { CloseHandle(self.h_map); }
        }
    }
}

// ============================================================
//  BusProducer
// ============================================================
pub struct BusProducer {
    queue_name: String,
    h_map: HANDLE,
    h_mutex: HANDLE,
    h_items: HANDLE,
    h_spaces: HANDLE,
    ptr: *mut BusSharedQueue,
}

unsafe impl Send for BusProducer {}
unsafe impl Sync for BusProducer {}

impl BusProducer {
    pub fn connect_or_create(queue_name: &str, running: &AtomicBool) -> Result<Self> {
        if !running.load(Ordering::SeqCst) {
            return Err(anyhow!("shutdown перед connect_or_create для {}", queue_name));
        }

        let base = |sfx: &str| format!("Global\\KK_{}_{}", queue_name, sfx);
        let shm_n = cname(&base("SharedMemory"));
        let mut_n = cname(&base("Mutex"));
        let itm_n = cname(&base("Items"));
        let spc_n = cname(&base("Spaces"));

        unsafe {
            let invalid_handle: HANDLE = -1isize as HANDLE;
            let h_map = CreateFileMappingA(
                invalid_handle,
                ptr::null_mut(),
                PAGE_READWRITE,
                0,
                std::mem::size_of::<BusSharedQueue>() as u32,
                shm_n.as_ptr() as *const c_char,
            );
            if h_map.is_null() {
                return Err(anyhow!(
                    "CreateFileMappingA failed for {} (err {})",
                    queue_name,
                    GetLastError()
                ));
            }
            let created_now = GetLastError() != ERROR_ALREADY_EXISTS;

            let raw = MapViewOfFile(h_map, FILE_MAP_ALL_ACCESS, 0, 0,
                                    std::mem::size_of::<BusSharedQueue>());
            if raw.is_null() {
                CloseHandle(h_map);
                return Err(anyhow!("MapViewOfFile failed for {}", queue_name));
            }
            let ptr = raw as *mut BusSharedQueue;

            let h_mutex = CreateMutexA(ptr::null_mut(), 0, mut_n.as_ptr() as *const c_char);
            if h_mutex.is_null() {
                UnmapViewOfFile(raw);
                CloseHandle(h_map);
                return Err(anyhow!("CreateMutexA failed for {}", queue_name));
            }
            let h_items = CreateSemaphoreA(ptr::null_mut(), 0, BUS_QUEUE_CAPACITY as LONG,
                                            itm_n.as_ptr() as *const c_char);
            if h_items.is_null() {
                CloseHandle(h_mutex);
                UnmapViewOfFile(raw);
                CloseHandle(h_map);
                return Err(anyhow!("CreateSemaphoreA(items) failed for {}", queue_name));
            }
            let h_spaces = CreateSemaphoreA(ptr::null_mut(), BUS_QUEUE_CAPACITY as LONG,
                                            BUS_QUEUE_CAPACITY as LONG,
                                            spc_n.as_ptr() as *const c_char);
            if h_spaces.is_null() {
                CloseHandle(h_items);
                CloseHandle(h_mutex);
                UnmapViewOfFile(raw);
                CloseHandle(h_map);
                return Err(anyhow!("CreateSemaphoreA(spaces) failed for {}", queue_name));
            }

            let rm = WaitForSingleObject(h_mutex, INFINITE);
            if !wait_acquired(rm) {
                CloseHandle(h_spaces);
                CloseHandle(h_items);
                CloseHandle(h_mutex);
                UnmapViewOfFile(raw);
                CloseHandle(h_map);
                return Err(anyhow!("Init mutex wait failed: {}", rm));
            }
            let q = &mut *ptr;
            if created_now || q.initialized != 1 {
                q.initialized = 1;
                q.head = 0;
                q.tail = 0;
                q.capacity = BUS_QUEUE_CAPACITY as i32;
                println!("[BUS] [INIT] Очередь '{}' инициализирована.", queue_name);
            } else {
                println!(
                    "[BUS] [INIT] Очередь '{}' уже существует. head={} tail={}",
                    queue_name, q.head, q.tail
                );
            }
            ReleaseMutex(h_mutex);

            Ok(Self {
                queue_name: queue_name.to_string(),
                h_map,
                h_mutex,
                h_items,
                h_spaces,
                ptr,
            })
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish(&self,
                   source_service: &str, target_service: &str, message_type: &str,
                   user_id: &str,
                   source: &str, task_type: &str, query: &str,
                   causation_id: &str, correlation_id: &str) -> Result<()> {
        unsafe {
            let ws = WaitForSingleObject(self.h_spaces, INFINITE);
            if ws != WAIT_OBJECT_0 {
                return Err(anyhow!("WaitForSingleObject(spaces) failed: {}", ws));
            }
            let wm = WaitForSingleObject(self.h_mutex, INFINITE);
            if !wait_acquired(wm) {
                ReleaseSemaphore(self.h_spaces, 1, ptr::null_mut());
                return Err(anyhow!("WaitForSingleObject(mutex) failed: {}", wm));
            }

            let q = &mut *self.ptr;
            let cap = if q.capacity > 0 { q.capacity } else { BUS_QUEUE_CAPACITY as i32 };
            let tail = q.tail;
            let idx = (tail % cap) as usize;

            let msg = &mut q.messages[idx];
            std::ptr::write_bytes(
                msg as *mut BusMessage as *mut u8,
                0,
                std::mem::size_of::<BusMessage>(),
            );

            let message_id = build_id("msg");
            let corr_id = if correlation_id.is_empty() {
                build_id("corr")
            } else {
                correlation_id.to_string()
            };

            safe_copy_bytes(&mut msg.metadata.message_id, &message_id);
            safe_copy_bytes(&mut msg.metadata.correlation_id, &corr_id);
            safe_copy_bytes(&mut msg.metadata.causation_id, causation_id);
            msg.metadata.created_at_ms = current_time_ms();
            safe_copy_bytes(&mut msg.metadata.source_service, source_service);
            safe_copy_bytes(&mut msg.metadata.target_service, target_service);
            safe_copy_bytes(&mut msg.metadata.message_type, message_type);
            msg.metadata.schema_version = 1;
            safe_copy_bytes(&mut msg.metadata.user_id, user_id);

            safe_copy_bytes(&mut msg.payload.source, source);
            safe_copy_bytes(&mut msg.payload.task_type, task_type);
            safe_copy_bytes(&mut msg.payload.query, query);

            q.tail = (tail + 1) % cap;
            ReleaseMutex(self.h_mutex);
            ReleaseSemaphore(self.h_items, 1, ptr::null_mut());
            Ok(())
        }
    }
}

impl Drop for BusProducer {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                UnmapViewOfFile(self.ptr as *const c_void);
            }
            if !self.h_spaces.is_null() { CloseHandle(self.h_spaces); }
            if !self.h_items.is_null() { CloseHandle(self.h_items); }
            if !self.h_mutex.is_null() { CloseHandle(self.h_mutex); }
            if !self.h_map.is_null() { CloseHandle(self.h_map); }
        }
    }
}
