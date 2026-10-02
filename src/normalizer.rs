/// Заменяет латинские омоглифы (похожие по начертанию буквы) на русские эквиваленты.
/// Также заменяет букву 'ё' на 'е'.
pub fn normalize_homoglyph(c: char) -> char {
    match c {
        'a' => 'а',
        'c' => 'с',
        'e' => 'е',
        'o' => 'о',
        'p' => 'р',
        'x' => 'х',
        'y' => 'у',
        'k' => 'к',
        'm' => 'м',
        't' => 'т',
        'h' => 'н',
        'b' => 'ь',
        'ё' => 'е',
        other => other,
    }
}

/// Очищает строку:
/// 1. Переводит все символы в нижний регистр.
/// 2. Слова без цифр прогоняет через омоглифы.
/// 3. Слова с цифрами (артикулы вроде h205, 2rs, b10) оставляет как есть.
/// 4. Любые разделители (включая _ и -) превращает в пробелы.
pub fn clean_text(input: &str) -> String {
    let lower = input.to_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut word = String::new();

    let flush = |word: &mut String, out: &mut String| {
        if word.chars().any(|c| c.is_numeric()) {
            out.push_str(word);
        } else {
            for c in word.chars() {
                out.push(normalize_homoglyph(c));
            }
        }
        word.clear();
    };

    for c in lower.chars() {
        if c.is_alphanumeric() {
            word.push(c);
        } else {
            flush(&mut word, &mut out);
            out.push(' ');
        }
    }
    flush(&mut word, &mut out);
    out
}

/// Разбивает очищенную строку на отдельные поисковые токены (слова и числа).
pub fn tokenize(input: &str) -> Vec<String> {
    clean_text(input)
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_homoglyphs() {
        let cleaned = clean_text("мaсло cмазка бoлт тpуба");
        assert_eq!(cleaned, "масло смазка болт труба");
    }

    #[test]
    fn test_tokenization_and_special_chars() {
        let tokens = tokenize("Подшипник 6205-2RS / (ГОСТ 205); кол-во: 10шт.");
        assert_eq!(
            tokens,
            vec!["подшипник", "6205", "2rs", "гост", "205", "кол", "во", "10шт"]
        );
    }

    #[test]
    fn test_yo_to_e() {
        assert_eq!(tokenize("Шестерня ведомая крепёж"), vec!["шестерня", "ведомая", "крепеж"]);
    }

    #[test]
    fn test_articles_keep_latin() {
        assert_eq!(tokenize("Фильтр h205 b10"), vec!["фильтр", "h205", "b10"]);
    }

    #[test]
    fn test_empty_query_has_no_tokens() {
        assert!(tokenize("   - / ").is_empty());
    }

    #[test]
    fn test_bid_number_normalization() {
        // Номер заявки вида ЦЕХ_М-00546 нормализуется в поисковый вид
        let cleaned = clean_text("ЦЕХ_М-00546");
        assert_eq!(cleaned, "цех м 00546");
    }
}
