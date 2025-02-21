//! Implementation of a random number generator compatible with `dbgen`.

#[derive(Default, Debug, Clone, Copy)]
pub struct TpchRng {
    seed: i64,
    uses: i64,
    uses_per_row: i64,
}

impl TpchRng {
    /// Constants as defined in https://github.com/electrum/tpch-dbgen/blob/master/rnd.h
    const MULTIPLIER: i64 = 16807;
    const MODULUS: i64 = 2147483647;
    const DEFAULT_SEED: i64 = 19650218;

    /// Creates a new random number generator with a given initial seed
    /// and the number of random values per row.
    pub fn new(seed: i64, uses: i64) -> Self {
        Self {
            seed,
            uses: 0,
            uses_per_row: uses,
        }
    }

    /// Returns a random value uniformly picked from the range specified by
    /// `lower_bound` and `upper_bound` both inclusive.
    pub fn random(&mut self, lower_bound: i32, upper_bound: i32) -> i32 {
        let _ = self.next_seed();

        // This code is buggy but must be this way because we aim to have
        // bug-for-bug with the original C implementation; thanks to the Trino
        // OG's for pointing the way; Lisan al Gaib.
        let range = (upper_bound - lower_bound + 1) as f64;
        let value_in_range = ((1. * self.seed as f64) / Self::MODULUS as f64) * range;

        lower_bound + value_in_range as i32
    }

    /// Updates the random number generator internal state by updating its internal
    /// seed and incrementing the usage counter.
    fn next_seed(&mut self) -> i64 {
        debug_assert!(
            self.uses <= self.uses_per_row,
            "expected random to be used at most {} times per row but it was used {} times",
            self.uses_per_row,
            self.uses
        );
        self.seed = (self.seed * Self::MULTIPLIER) % Self::MODULUS;
        // Increment the "use" counter for the rng.
        self.uses += 1;
        self.seed
    }

    /// Advance the seed by a specific number of rows, essentially bumps
    /// the rng state to next partition.
    fn prepare_next_partition(&mut self, partition_size: i64) {
        if self.uses != 0 {
            self.prepare_next_row();
        }

        self.advance(self.uses_per_row * partition_size);
    }

    /// Advances the seed to start the sequence for the next row.
    fn prepare_next_row(&mut self) {
        self.advance(self.uses_per_row - self.uses);
        self.uses = 0;
    }

    /// Advance the seed after `count` calls.
    fn advance(&mut self, count: i64) {
        let mut count = count;
        let mut multiplier = Self::MULTIPLIER;
        while count > 0 {
            if count % 2 != 0 {
                self.seed = (multiplier * self.seed) % Self::MODULUS;
            }
            count /= 2;
            multiplier = (multiplier * multiplier) % Self::MODULUS;
        }
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
            seed,
            min_len: (avg_len as f64 * Self::LOW_LENGTH_MULTIPLIER).trunc() as usize,
            max_len: (avg_len as f64 * Self::HIGH_LENGTH_MULTIPLIER).trunc() as usize,
        }
    }

    /// Returns the next random string.
    pub fn next(&self) {}
}

#[cfg(test)]
mod tests {
    use super::TpchRng;

    #[test]
    fn can_build_a_valid_rng() {
        let mut rng = TpchRng::new(933588178, i32::MAX as i64);

        for _ in 0..1024 {
            rng.random(0, 1024);
        }
    }
}
