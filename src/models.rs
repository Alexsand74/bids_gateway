use serde::{Deserialize, Serialize};

// =========================================================================
// КОНСТАНТЫ ПРОТОКОЛА RAM BUS (совпадают с остальными сервисами и C++ MessageBus)
// =========================================================================
pub const BUS_QUEUE_CAPACITY: usize = 64;
pub const BUS_MAX_ID_LEN: usize = 64;
pub const BUS_MAX_SERVICE_LEN: usize = 32;
pub const BUS_MAX_TYPE_LEN: usize = 64;
pub const BUS_MAX_USER_LEN: usize = 32;
pub const BUS_MAX_QUERY_LEN: usize = 524288;
pub const BUS_MAX_SOURCE_LEN: usize = 32;
pub const BUS_MAX_TASK_TYPE_LEN: usize = 32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BusMessageMetadata {
    pub message_id: [u8; BUS_MAX_ID_LEN],
    pub correlation_id: [u8; BUS_MAX_ID_LEN],
    pub causation_id: [u8; BUS_MAX_ID_LEN],
    pub created_at_ms: i64,
    pub source_service: [u8; BUS_MAX_SERVICE_LEN],
    pub target_service: [u8; BUS_MAX_SERVICE_LEN],
    pub message_type: [u8; BUS_MAX_TYPE_LEN],
    pub schema_version: i32,
    pub user_id: [u8; BUS_MAX_USER_LEN],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BusMessagePayload {
    pub source: [u8; BUS_MAX_SOURCE_LEN],
    pub task_type: [u8; BUS_MAX_TASK_TYPE_LEN],
    pub query: [u8; BUS_MAX_QUERY_LEN],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BusMessage {
    pub metadata: BusMessageMetadata,
    pub payload: BusMessagePayload,
}

#[repr(C)]
pub struct BusSharedQueue {
    pub initialized: i32,
    pub head: i32,
    pub tail: i32,
    pub capacity: i32,
    pub messages: [BusMessage; BUS_QUEUE_CAPACITY],
}

#[derive(Debug, Clone)]
pub struct HeapTask {
    pub source_service: String,
    pub target_service: String,
    pub message_type: String,
    pub user_id: String,
    pub correlation_id: String,
    pub causation_id: String,
    pub message_id: String,
    pub source: String,
    pub task_type: String,
    pub payload_query: String,
}

// =========================================================================
// ЗАЯВКА В ПАМЯТИ
// Реальная структура supply_requests.json от orekeeper (проверено на данных):
//   { "ST1": { "Number": "ЦЕХ_М-00546", "Date": "...", "Sklad": "...",
//              "Name": "...", "Zakazal": "...", "Manager": "...",
//              "ZakazPodr": "040266 Цех ...", "ZakupPodr": "040176 ...",
//              "Status": "...", "Srochnost": "...", "Details": "...",
//              "WishDate": "...", "PlanDate": "...", "CompDate": "...",
//              "Tovars": { "STT1": { "StrNum": "1", "Tovar": "...",
//                                    "Tovar2": "...", "Kolvo": "...",
//                                    "Edizm": "...", "URL": "..." }, ... } } }
// =========================================================================
#[derive(Debug, Clone)]
pub struct BidItem {
    // исходные поля
    pub number: String,    // Number, например "ЦЕХ_М-00546"
    pub date: String,     // Date
    pub firm: String,     // ZakazPodr — заказывающее подразделение (организация)
    pub firm2: String,    // ZakupPodr — закупающее подразделение
    pub name: String,     // Name — название заявки
    pub comment: String,  // Details — комментарий
    pub goods: String,    // все позиции товаров: "Tovar; Tovar2; ..."
    pub items_count: usize,
    // происхождение записи
    pub month: String,
    pub session: String,
    // нормализованные поля для поиска
    pub number_norm: String,
    pub firm_tokens: Vec<String>,
    pub firm2_tokens: Vec<String>,
    pub name_tokens: Vec<String>,
    pub comment_tokens: Vec<String>,
    pub goods_tokens: Vec<String>,
    pub all_text: String,
}

// =========================================================================
// ВХОДЯЩИЙ ПОИСКОВЫЙ ЗАПРОС (JSON внутри payload.query)
// =========================================================================
#[derive(Debug, Default, Deserialize)]
pub struct SearchQueryParams {
    #[serde(default)]
    pub keyword: String,
    /// "number" | "firm" | "goods" | "name" | "comment" | "any" (по умолчанию)
    #[serde(default)]
    pub field: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SearchQueryRequest {
    #[allow(dead_code)]
    #[serde(default)]
    pub task_id: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    pub source: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub params: Option<SearchQueryParams>,
}

// =========================================================================
// ИСХОДЯЩИЙ ОТВЕТ В ШИНУ
// =========================================================================
#[derive(Debug, Serialize, Clone)]
pub struct BidResultItem {
    pub number: String,
    pub date: String,
    pub firm: String,
    pub firm2: String,
    pub name: String,
    pub comment: String,
    pub items_count: usize,
    /// Первые позиции товаров (обрезаны до 200 символов, для показа пользователю)
    pub goods_preview: String,
    pub month: String,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub status: String,
    pub total: usize,
    pub truncated: bool,
    pub result: Vec<BidResultItem>,
}
