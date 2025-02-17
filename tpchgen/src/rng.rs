//! Implementation of a random number generator compatible with `dbgen`.

/// Constants as defined in https://github.com/electrum/tpch-dbgen/blob/master/rnd.h
const MULTIPLIER: i64 = 16807;
const MODULUS: i64 = 2147483647;
const DEFAULT_SEED: i64 = 19650218;

#[derive(Default, Debug, Clone, Copy)]
pub struct TpchRng {
    seed: i64,
}

impl TpchRng {
    pub fn new(seed: i64) -> Self {
        Self { seed }
    }

    /// Returns the next random number.
    pub fn next(&mut self) -> i64 {
        self.seed = (self.seed * MULTIPLIER) % MODULUS;
        self.seed
    }

    /// Returns a random value sampled uniformly from the specified range.
    pub fn uniform(&mut self, min: i64, max: i64) -> i64 {
        debug_assert!(
            min <= max,
            "min {min} must be less than or equal to max {max}"
        );
        min + (self.next() % (max - min + 1))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TpchAlphanumRng {
    seed: i64,
    min_len: usize,
    max_len: usize,
}

impl TpchAlphanumRng {
    /// Multiplier for small length strings.
    const LOW_LENGTH_MULTIPLIER: f64 = 0.4;
    /// Multiplier for larger strings.
    const HIGH_LENGTH_MULTIPLIER: f64 = 1.6;
    /// Dictionary used for generation.
    const DICT: &'static [char] = &[
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        ' ', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q',
        'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', ',',
    ];

    /// Create a new random alphanumeric string generator.
    pub fn new(seed: i64, avg_len: usize) -> Self {
        Self {
            seed: seed,
            min_len: (avg_len as f64 * Self::LOW_LENGTH_MULTIPLIER).trunc() as usize,
            max_len: (avg_len as f64 * Self::HIGH_LENGTH_MULTIPLIER).trunc() as usize,
        }
    }

    /// Returns the next random string.
    pub fn next(&self) {}
}
