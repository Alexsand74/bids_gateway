//! Поиск по заявкам.
//! field: "number" | "firm" | "goods" | "name" | "comment" | любое другое = везде.

use crate::models::BidItem;
use crate::normalizer::{clean_text, tokenize};

pub fn search_bids(snapshot: &[BidItem], query: &str, field: &str) -> Vec<&BidItem> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    // Поиск по номеру заявки — вхождение нормализованной строки
    // ("ЦЕХ_М-00546" нормализуется в "цех м 00546", ищется и "00546", и "М-00546")
    if field.eq_ignore_ascii_case("number") {
        let needle = clean_text(trimmed);
        if needle.is_empty() {
            return Vec::new();
        }
        return snapshot
            .iter()
            .filter(|b| !b.number_norm.is_empty() && b.number_norm.contains(&needle))
            .collect();
    }

    // Поиск по словам: каждый токен запроса должен найтись в выбранном поле
    let tokens = tokenize(trimmed);
    if tokens.is_empty() {
        return Vec::new();
    }

    snapshot
        .iter()
        .filter(|b| {
            tokens.iter().all(|t| match field {
                // организация: и заказывающее, и закупающее подразделение
                f if f.eq_ignore_ascii_case("firm") => b
                    .firm_tokens
                    .iter()
                    .chain(b.firm2_tokens.iter())
                    .any(|ft| ft.contains(t.as_str())),
                // товары заявки (Tovar/Tovar2)
                f if f.eq_ignore_ascii_case("goods") => {
                    b.goods_tokens.iter().any(|ft| ft.contains(t.as_str()))
                }
                f if f.eq_ignore_ascii_case("name") => {
                    b.name_tokens.iter().any(|ft| ft.contains(t.as_str()))
                }
                f if f.eq_ignore_ascii_case("comment") => {
                    b.comment_tokens.iter().any(|ft| ft.contains(t.as_str()))
                }
                // "any" и всё остальное — ищем по всем полям сразу
                _ => b.all_text.contains(t.as_str()),
            })
        })
        .collect()
}
