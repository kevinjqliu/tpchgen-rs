//! Shared command-line argument parsing.

pub(crate) fn parse_row_group_bytes(value: &str) -> Result<i64, String> {
    let bytes = parse_size::parse_size(value).map_err(|err| err.to_string())?;
    if bytes == 0 {
        return Err("must be greater than zero".to_string());
    }

    i64::try_from(bytes).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_group_bytes_parses_and_validates_values() {
        assert_eq!(parse_row_group_bytes("1"), Ok(1));
        assert_eq!(parse_row_group_bytes(&i64::MAX.to_string()), Ok(i64::MAX));
        assert_eq!(parse_row_group_bytes("8mb"), Ok(8 * 1000 * 1000));
        assert_eq!(parse_row_group_bytes("8MiB"), Ok(8 * 1024 * 1024));
        assert!(parse_row_group_bytes(&(i64::MAX as u64 + 1).to_string()).is_err());
    }
}
