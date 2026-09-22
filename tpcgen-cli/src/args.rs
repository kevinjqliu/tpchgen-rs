//! Shared command-line argument parsing.

pub(crate) fn parse_positive_bytes(value: &str) -> Result<i64, String> {
    let bytes = value.parse::<i64>().map_err(|err| err.to_string())?;
    if bytes <= 0 {
        return Err("must be greater than zero".to_string());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_bytes_enforces_valid_range() {
        assert_eq!(parse_positive_bytes("1"), Ok(1));
        assert_eq!(parse_positive_bytes(&i64::MAX.to_string()), Ok(i64::MAX));
        assert_eq!(
            parse_positive_bytes("0"),
            Err("must be greater than zero".to_string())
        );
        assert_eq!(
            parse_positive_bytes("-1"),
            Err("must be greater than zero".to_string())
        );
        assert!(parse_positive_bytes(&(i64::MAX as u64 + 1).to_string()).is_err());
    }
}
