use std::fmt;

/// Represents a 'Decimal(15, 2)' value interally represented an an i64.
///
/// A 'decimal' column should be able to fit any values in the the range
/// [-9_999_999_999.99, +9_999_999_999.99] in increments of 0.01.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Decimal(pub i64);

impl Decimal {
    pub const ZERO: Decimal = Decimal(0);

    /// Converts the decimal value to an f64.
    ///
    /// This is a potentially lossy conversion.
    pub const fn as_f64(&self) -> f64 {
        self.0 as f64 / 100.0
    }

    /// Returns if this decimal is negative.
    const fn is_negative(&self) -> bool {
        self.0.is_negative()
    }

    /// Returns the digits before the decimal point.
    const fn int_digits(&self) -> i64 {
        (self.0 / 100).abs()
    }

    /// Returns the digits after the decimal point.
    const fn decimal_digits(&self) -> i64 {
        (self.0 % 100).abs()
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}.{:0>2}",
            if self.is_negative() { "-" } else { "" },
            self.int_digits(),
            self.decimal_digits()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_format() {
        struct TestCase {
            decimal: Decimal,
            expected: &'static str,
        }

        let test_cases = [
            TestCase {
                decimal: Decimal(0),
                expected: "0.00",
            },
            TestCase {
                decimal: Decimal(1),
                expected: "0.01",
            },
            TestCase {
                decimal: Decimal(10),
                expected: "0.10",
            },
            TestCase {
                decimal: Decimal(100),
                expected: "1.00",
            },
            TestCase {
                decimal: Decimal(1000),
                expected: "10.00",
            },
            TestCase {
                decimal: Decimal(1234),
                expected: "12.34",
            },
            TestCase {
                decimal: Decimal(-1),
                expected: "-0.01",
            },
            TestCase {
                decimal: Decimal(-10),
                expected: "-0.10",
            },
            TestCase {
                decimal: Decimal(-100),
                expected: "-1.00",
            },
            TestCase {
                decimal: Decimal(-1000),
                expected: "-10.00",
            },
            // Max according to spec.
            TestCase {
                decimal: Decimal(999_999_999_999),
                expected: "9999999999.99",
            },
            // Min according to spec.
            TestCase {
                decimal: Decimal(-999_999_999_999),
                expected: "-9999999999.99",
            },
        ];

        for test_case in test_cases {
            let formatted = test_case.decimal.to_string();
            assert_eq!(
                test_case.expected, formatted,
                "input decimal: {:?}",
                test_case.decimal,
            );
        }
    }
}
