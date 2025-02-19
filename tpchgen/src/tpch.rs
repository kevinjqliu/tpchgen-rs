mod constants {}

/// Scale factors used to specify the size of the dataset to generate
/// a scale factor of 1 is approximately 1GB and is considered the smallest
/// one.
#[derive(Default, Debug, Clone, Copy)]
pub struct ScaleFactor(usize);

impl std::fmt::Display for ScaleFactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            1 => write!(f, "1 GB"),
            10 => write!(f, "10 GB"),
            30 => write!(f, "30 GB"),
            100 => write!(f, "100 GB"),
            300 => write!(f, "300 GB"),
            1000 => write!(f, "1000 GB"),
            3000 => write!(f, "3000 GB"),
            10000 => write!(f, "10000 GB"),
            _ => write!(f, "{}", self.0),
        }
    }
}

impl ScaleFactor {
    /// Creates a new scale factor value and it must be in the set of allowed
    /// factors by the spec (1, 10, 30, 300, 1000, 3000, 10000, 30000, 100000).
    pub fn new(sf: usize) -> Self {
        debug_assert!(matches!(
            sf,
            1 | 10 | 30 | 100 | 300 | 1000 | 3000 | 10000 | 30000 | 100000
        ));

        Self(sf)
    }
}
