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
//              "Status": "Выполнена", "Srochnost": "Текущая", "Details": "...",
//              "WishDate": "...", "PlanDate": "...", "CompDate": "...",
//              "Tovars": { "STT1": { "StrNum": "1", "Tovar": "...",
//                                    "Tovar2": "...", "Kolvo": "...",
//                                    "Edizm": "...", "URL": "..." }, ... } } }
// =========================================================================

/// Одна товарная позиция заявки (для команды «Товары <номер>»).
/// name = Tovar (полное название из номенклатуры), если не пуст; иначе Tovar2.
#[derive(Debug, Clone)]
pub struct BidTovarLine {
    pub str_num: u32,
    pub name: String,
    pub kolvo: String,
    pub edizm: String,
}

#[derive(Debug, Clone)]
pub struct BidItem {
    // исходные поля
    pub number: String,     // Number, например "ЦЕХ_М-00546"
    pub date: String,       // Date
    pub firm: String,       // ZakazPodr — заказывающее подразделение (организация)
    pub firm2: String,      // ZakupPodr — закупающее подразделение
    pub name: String,       // Name — название заявки
    pub comment: String,    // Details — комментарий
    // НОВОЕ: статус и реквизиты карточки заявки
    pub status: String,     // Status, например "Выполнена"
    pub srochnost: String,  // Srochnost, например "Текущая"
    pub sklad: String,      // Sklad — склад / объект назначения
    pub zakazal: String,    // Zakazal — кто заказал
    pub manager: String,    // Manager — ответственный менеджер
    pub wish_date: String,  // WishDate — требуемая дата
    pub plan_date: String, // PlanDate — плановая дата
    pub comp_date: String, // CompDate — дата исполнения (может быть пустой!)
    // товары
    pub goods: String,                  // все позиции для поиска: "Tovar; Tovar2; ..."
    pub items_count: usize,            // число позиций (по записям Tovars)
    pub tovar_lines: Vec<BidTovarLine>, // структурные позиции для показа
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

impl BidItem {
    /// Полная карточка заявки для ответа на действие "get_bid".
    pub fn to_details(&self) -> BidDetails {
        BidDetails {
            number: self.number.clone(),
            date: self.date.clone(),
            name: self.name.clone(),
            status: self.status.clone(),
            srochnost: self.srochnost.clone(),
            sklad: self.sklad.clone(),
            zakazal: self.zakazal.clone(),
            manager: self.manager.clone(),
            firm: self.firm.clone(),
            firm2: self.firm2.clone(),
            wish_date: self.wish_date.clone(),
            plan_date: self.plan_date.clone(),
            comp_date: self.comp_date.clone(),
            comment: self.comment.clone(),
            items_count: self.items_count,
            month: self.month.clone(),
        }
    }
}

// =========================================================================
// ВХОДЯЩИЙ ЗАПРОС (JSON внутри payload.query)
// action: "search_bids" (по умолчанию) | "get_bid" | "get_bid_tovars"
// =========================================================================
#[derive(Debug, Default, Deserialize)]
pub struct SearchQueryParams {
    #[serde(default)]
    pub keyword: String,
    /// "number" | "firm" | "goods" | "name" | "comment" | "any" (по умолчанию)
    #[serde(default)]
    pub field: Option<String>,
    /// Номер заявки для действий get_bid / get_bid_tovars
    #[serde(default)]
    pub number: Option<String>,
    /// Номер страницы товаров (1-based) для get_bid_tovars
    #[serde(default)]
    pub page: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct SearchQueryRequest {
    #[allow(dead_code)]
    #[serde(default)]
    pub task_id: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub params: Option<SearchQueryParams>,
}

// =========================================================================
// ИСХОДЯЩИЕ ОТВЕТЫ В ШИНУ
// =========================================================================

/// Позиция в списке результатов поиска (теперь со статусом).
#[derive(Debug, Serialize, Clone)]
pub struct BidResultItem {
    pub number: String,
    pub date: String,
    pub firm: String,
    pub firm2: String,
    pub name: String,
    pub comment: String,
    pub status: String,
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

/// Полная карточка заявки (ответ на "get_bid").
#[derive(Debug, Serialize, Clone)]
pub struct BidDetails {
    pub number: String,
    pub date: String,
    pub name: String,
    pub status: String,
    pub srochnost: String,
    pub sklad: String,
    pub zakazal: String,
    pub manager: String,
    pub firm: String,
    pub firm2: String,
    pub wish_date: String,
    pub plan_date: String,
    pub comp_date: String,
    pub comment: String,
    pub items_count: usize,
    pub month: String,
}

#[derive(Debug, Serialize)]
pub struct BidDetailsResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<BidDetails>,
}

/// Одна товарная позиция в ответе "get_bid_tovars".
#[derive(Debug, Serialize, Clone)]
pub struct BidTovarLineOut {
    pub str_num: u32,
    pub name: String,
    pub kolvo: String,
    pub edizm: String,
}

/// Страница товарных позиций заявки (ответ на "get_bid_tovars").
#[derive(Debug, Serialize)]
pub struct BidTovarsResponse {
    pub status: String,
    pub number: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub total: usize,
    pub page: usize,
    pub pages: usize,
    pub result: Vec<BidTovarLineOut>,
}
