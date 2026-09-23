//! Shared command-line argument parsing.

/// Parses a positive byte count using case-insensitive GNU size suffixes.
///
/// * `KB`, `MB`, `GB`, `TB`: powers of 1000
/// * `K`, `M`, `G`, `T` and `KiB`, `MiB`, `GiB`, `TiB`: powers of 1024
pub(crate) fn parse_row_group_bytes(value: &str) -> Result<i64, String> {
    if value.starts_with('-') {
        return Err("must be greater than zero".to_string());
    }

    let number_end = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(number_end);

    let number = number.parse::<i64>().map_err(|error| error.to_string())?;
    if number == 0 {
        return Err("must be greater than zero".to_string());
    }

    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1_000,
        "k" | "kib" => 1 << 10,
        "mb" => 1_000_000,
        "m" | "mib" => 1 << 20,
        "gb" => 1_000_000_000,
        "g" | "gib" => 1 << 30,
        "tb" => 1_000_000_000_000,
        "t" | "tib" => 1_i64 << 40,
        _ => return Err(format!("unknown byte unit '{suffix}'")),
    };

    number
        .checked_mul(multiplier)
        .ok_or_else(|| "byte value is too large".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_group_bytes_parses_and_validates_values() {
        assert_eq!(parse_row_group_bytes("1"), Ok(1));
        assert_eq!(parse_row_group_bytes(&i64::MAX.to_string()), Ok(i64::MAX));
        assert_eq!(parse_row_group_bytes("8mb"), Ok(8 * 1000 * 1000));
        assert_eq!(parse_row_group_bytes("8M"), Ok(8 * 1024 * 1024));
        assert_eq!(parse_row_group_bytes("8MiB"), Ok(8 * 1024 * 1024));
        assert_eq!(
            parse_row_group_bytes("-1"),
            Err("must be greater than zero".to_string())
        );
        assert_eq!(
            parse_row_group_bytes("0MB"),
            Err("must be greater than zero".to_string())
        );
        assert!(parse_row_group_bytes(&(i64::MAX as u64 + 1).to_string()).is_err());
        assert!(parse_row_group_bytes(&format!("{}KB", i64::MAX)).is_err());
        assert!(parse_row_group_bytes("MiB").is_err());
        assert!(parse_row_group_bytes("1.5MB").is_err());
        assert!(parse_row_group_bytes("+1").is_err());
        assert!(parse_row_group_bytes("8watts").is_err());
    }
}
