use std::collections::HashMap;
use std::io::{self, BufRead};
use thiserror::Error;

/// Embedded TPCH distributions seed file as found in dbgen's implementation.
const DISTS_SEED: &str = include_str!("dists.dss");

#[derive(Error, Debug)]
pub enum DistParserError {
    #[error("Invalid distribution format: {0}")]
    InvalidFormat(String),
    #[error("Missing count in distribution")]
    MissingCount,
    #[error("Distribution already exists: {0}")]
    DuplicateDistribution(String),
    #[error("Empty distribution: {0}")]
    EmptyDistribution(String),
    #[error("Invalid weight value: {0}")]
    InvalidWeight(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
}

#[derive(Debug, Clone)]
pub struct Distribution {
    pub name: String,
    pub count: usize,
    pub entries: Vec<DistributionEntry>,
    pub dist_type: DistributionType,
}

#[derive(Debug, Clone)]
pub struct DistributionEntry {
    pub value: String,
    pub weight: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DistributionType {
    Regular,
    // For nations where weights are adjustments
    Adjustment,
    // For colors which can't be used with pick_str
    Restricted,
    // For sentence structure rules.
    Grammar,
    // For text generated components.
    TextGen,
}

impl std::fmt::Display for Distribution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "| {} | {} | {:?} | {:?} |",
            self.name,
            self.count,
            self.dist_type,
            self.entries
                .iter()
                .map(|entry| format!("({}, {})", entry.value, entry.weight))
        )
    }
}

impl Distribution {
    pub fn sample<R: rand::Rng>(&self, rng: &mut R) -> &str {
        match self.dist_type {
            DistributionType::Regular | DistributionType::Restricted => self.sample_weighted(rng),
            DistributionType::Adjustment => self.sample_adjusted(rng),
            DistributionType::Grammar => self.sample_grammar(rng),
            DistributionType::TextGen => self.sample_text_gen(rng),
        }
    }

    fn sample_weighted<R: rand::Rng>(&self, rng: &mut R) -> &str {
        let total_weight: i32 = self.entries.iter().map(|e| e.weight).sum();
        let mut choice = rng.random_range(0..total_weight);

        for entry in &self.entries {
            if choice < entry.weight {
                return &entry.value;
            }
            choice -= entry.weight;
        }
        &self.entries[0].value
    }

    fn sample_adjusted<R: rand::Rng>(&self, rng: &mut R) -> &str {
        // Special handling for nations distribution where weights are adjustments.
        let base_weight = 10;
        let total_weight: i32 = self.entries.iter().map(|e| base_weight + e.weight).sum();

        let mut choice = rng.random_range(0..total_weight);
        for entry in &self.entries {
            let adjusted_weight = (base_weight + entry.weight).max(1);
            if choice < adjusted_weight {
                return &entry.value;
            }
            choice -= adjusted_weight;
        }
        &self.entries[0].value
    }

    fn sample_grammar<R: rand::Rng>(&self, rng: &mut R) -> &str {
        // For grammar rules like "N V T", "N V P T", etc.
        self.sample_weighted(rng)
    }

    fn sample_text_gen<R: rand::Rng>(&self, _rng: &mut R) -> &str {
        // For generating text components (nouns, verbs, etc.)
        // TODO: Implement more sophisticated text generation
        self.sample_weighted(_rng)
    }
}

#[derive(Debug, Default)]
pub struct DistributionParser {
    distributions: HashMap<String, Distribution>,
}

impl DistributionParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn parse<R: BufRead>(&mut self, reader: R) -> Result<(), DistParserError> {
        let mut current_dist: Option<Distribution> = None;
        let mut current_lines: Vec<String> = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();

            // Skip comments and empty lines when not in a distribution
            if current_dist.is_none() && (trimmed.starts_with('#') || trimmed.is_empty()) {
                continue;
            }

            if trimmed.starts_with("BEGIN ") {
                // Start new distribution
                let name = trimmed["BEGIN ".len()..].trim().to_string();
                if self.distributions.contains_key(&name) {
                    return Err(DistParserError::DuplicateDistribution(name));
                }
                current_dist = Some(Distribution {
                    name,
                    count: 0,
                    entries: Vec::new(),
                    dist_type: DistributionType::Regular,
                });
                current_lines.clear();
            } else if trimmed.starts_with("END ") {
                // Finalize current distribution
                if let Some(dist) = current_dist.take() {
                    self.validate_and_store_distribution(dist, &current_lines)?;
                }
                current_lines.clear();
            } else if !trimmed.is_empty() {
                current_lines.push(trimmed.to_string());
            }
        }

        Ok(())
    }

    fn validate_and_store_distribution(
        &mut self,
        mut dist: Distribution,
        lines: &[String],
    ) -> Result<(), DistParserError> {
        // Set distribution type based on name
        dist.dist_type = self.determine_distribution_type(dist.name.as_str());

        if lines.is_empty() {
            return Err(DistParserError::EmptyDistribution(dist.name));
        }

        // Process lines
        for line in lines {
            if line.starts_with("COUNT|") {
                let count = line
                    .split('|')
                    .nth(1)
                    .ok_or_else(|| DistParserError::InvalidFormat("Invalid COUNT format".into()))?
                    .parse()
                    .map_err(|_| DistParserError::InvalidFormat("Invalid COUNT value".into()))?;
                dist.count = count;
            } else if !line.starts_with('#') {
                let parts: Vec<&str> = line.split('|').collect();
                if parts.len() >= 2 {
                    let weight = parts[1].trim().parse::<i32>().map_err(|_| {
                        DistParserError::InvalidWeight(format!(
                            "Invalid weight '{}' in distribution '{}'",
                            parts[1], dist.name
                        ))
                    })?;

                    if dist.dist_type == DistributionType::Regular && weight <= 0 {
                        return Err(DistParserError::InvalidWeight(format!(
                            "Weight '{}' must be positive in regular distributions '{}'",
                            weight, dist.name
                        )));
                    }

                    let entry = DistributionEntry {
                        value: parts[0].to_string(),
                        weight,
                    };
                    dist.entries.push(entry);
                }
            }
        }

        // Validate
        if dist.count == 0 {
            return Err(DistParserError::MissingCount);
        }
        if dist.entries.len() != dist.count {
            return Err(DistParserError::InvalidFormat(format!(
                "Entry count mismatch: expected {}, got {}",
                dist.count,
                dist.entries.len()
            )));
        }

        self.distributions.insert(dist.name.clone(), dist);
        Ok(())
    }

    pub fn distribution(&self, name: &str) -> Option<&Distribution> {
        self.distributions.get(name)
    }
    fn determine_distribution_type(&self, name: &str) -> DistributionType {
        match name {
            "nations" => DistributionType::Adjustment,
            "colors" => DistributionType::Restricted,
            "grammar" | "np" | "vp" => DistributionType::Grammar,
            "nouns" | "verbs" | "adverbs" | "adjectives" | "articles" | "prepositions"
            | "auxillaries" | "terminators" => DistributionType::TextGen,
            _ => DistributionType::Regular,
        }
    }

    pub fn generate_comment<R: rand::Rng>(&self, rng: &mut R) -> Result<String, DistParserError> {
        let grammar = self
            .distribution("grammar")
            .ok_or(DistParserError::ParseError(
                "Grammar distribution not found".into(),
            ))?;

        let structure = grammar.sample(rng);
        let mut result = String::new();

        for token in structure.split_whitespace() {
            match token {
                "N" => {
                    let np = self.distribution("np").ok_or(DistParserError::ParseError(
                        "NP distribution not found".into(),
                    ))?;
                    result.push_str(np.sample(rng));
                }
                "V" => {
                    let vp = self.distribution("vp").ok_or(DistParserError::ParseError(
                        "VP distribution not found".into(),
                    ))?;
                    result.push_str(vp.sample(rng));
                }
                "T" => {
                    let term =
                        self.distribution("terminators")
                            .ok_or(DistParserError::ParseError(
                                "Terminators distribution not found".into(),
                            ))?;
                    result.push_str(term.sample(rng));
                }
                _ => result.push_str(token),
            }
            result.push(' ');
        }

        Ok(result.trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::io::Cursor;

    #[test]
    fn can_parse_simple_distribution() {
        let input = r#"
# Test distribution
BEGIN test_dist
COUNT|3
value1|10
value2|20
value3|30
END test_dist
"#;
        let mut parser = DistributionParser::new();
        parser
            .parse(Cursor::new(input))
            .expect("Encountered an error while processing input");

        let dist = parser
            .distribution("test_dist")
            .expect("Distribution 'test_dist' was not found");
        assert_eq!(dist.count, 3);
        assert_eq!(dist.entries.len(), 3);
        assert_eq!(dist.entries[0].value, "value1");
        assert_eq!(dist.entries[0].weight, 10);
    }

    #[test]
    fn can_parse_nations_distribution() {
        let input = r#"
BEGIN nations
COUNT|2
ALGERIA|0
ARGENTINA|1
END nations
"#;
        let mut parser = DistributionParser::new();
        parser
            .parse(Cursor::new(input))
            .expect("Encountered an error while processing input");

        let dist = parser
            .distribution("nations")
            .expect("Distribution 'nations' was not found");
        assert_eq!(dist.dist_type, DistributionType::Adjustment);
    }
    #[test]
    fn can_detect_distribution_type() {
        let input = r#"
BEGIN nations
COUNT|1
ALGERIA|0
END nations

BEGIN colors
COUNT|1
red|1
END colors

BEGIN grammar
COUNT|1
N V T|1
END grammar

BEGIN nouns
COUNT|1
packages|1
END nouns
"#;
        let mut parser = DistributionParser::new();
        parser.parse(Cursor::new(input)).unwrap();

        assert_eq!(
            parser.distribution("nations").unwrap().dist_type,
            DistributionType::Adjustment
        );
        assert_eq!(
            parser.distribution("colors").unwrap().dist_type,
            DistributionType::Restricted
        );
        assert_eq!(
            parser.distribution("grammar").unwrap().dist_type,
            DistributionType::Grammar
        );
        assert_eq!(
            parser.distribution("nouns").unwrap().dist_type,
            DistributionType::TextGen
        );
    }

    #[test]
    fn can_sample_with_weights() {
        let input = r#"
BEGIN test_weights
COUNT|3
rare|1
common|8
very_common|16
END test_weights
"#;
        let mut parser = DistributionParser::new();
        parser.parse(Cursor::new(input)).unwrap();

        let dist = parser.distribution("test_weights").unwrap();
        let mut rng = StdRng::seed_from_u64(42); // Fixed seed for reproducibility

        // Sample 1000 times and check distributions
        let mut counts = std::collections::HashMap::new();
        for _ in 0..1000 {
            let sample = dist.sample(&mut rng);
            *counts.entry(sample).or_insert(0) += 1;
        }

        // Verify that higher weights resulted in more samples
        assert!(counts.get("very_common").unwrap() > counts.get("common").unwrap());
        assert!(counts.get("common").unwrap() > counts.get("rare").unwrap());
    }

    #[test]
    fn can_sample_with_adjustments() {
        let input = r#"
BEGIN nations
COUNT|3
ALGERIA|0
CHINA|2
JAPAN|-2
END nations
"#;
        let mut parser = DistributionParser::new();
        parser.parse(Cursor::new(input)).unwrap();

        let dist = parser.distribution("nations").unwrap();
        let mut rng = StdRng::seed_from_u64(42);

        let mut counts = std::collections::HashMap::new();
        for _ in 0..1000 {
            let sample = dist.sample(&mut rng);
            *counts.entry(sample).or_insert(0) += 1;
        }

        counts
            .iter()
            .for_each(|count| println!("Found counts for {}", count.0));

        // Verify that adjustments affected sampling rates
        assert!(counts.get("CHINA").unwrap() > counts.get("ALGERIA").unwrap());
        assert!(counts.get("ALGERIA").unwrap() > counts.get("JAPAN").unwrap());
    }

    #[test]
    fn can_infer_nations_frequency_distribution() {
        let input = r#"
BEGIN nations
COUNT|5
FRANCE|3
CHINA|2
BRAZIL|1
ALGERIA|0
JAPAN|-2
END nations
"#;
        let mut parser = DistributionParser::new();
        parser.parse(Cursor::new(input)).unwrap();

        let dist = parser
            .distribution("nations")
            .expect("nations distribution should exist");
        let mut rng = StdRng::seed_from_u64(42);

        // Sample a large number of times to get stable distributions
        let mut counts = std::collections::HashMap::new();
        const SAMPLE_SIZE: usize = 10000;
        for _ in 0..SAMPLE_SIZE {
            let sample = dist.sample(&mut rng);
            *counts.entry(sample).or_insert(0) += 1;
        }

        println!("Sampling distribution: {:?}", counts);

        // Get all counts
        let france_count = *counts.get("FRANCE").unwrap_or(&0);
        let china_count = *counts.get("CHINA").unwrap_or(&0);
        let algeria_count = *counts.get("ALGERIA").unwrap_or(&0);
        let brazil_count = *counts.get("BRAZIL").unwrap_or(&0);
        let japan_count = *counts.get("JAPAN").unwrap_or(&0);

        // Calculate frequencies
        let france_freq = france_count as f64 / SAMPLE_SIZE as f64;
        let china_freq = china_count as f64 / SAMPLE_SIZE as f64;
        let algeria_freq = algeria_count as f64 / SAMPLE_SIZE as f64;
        let brazil_freq = brazil_count as f64 / SAMPLE_SIZE as f64;
        let japan_freq = japan_count as f64 / SAMPLE_SIZE as f64;

        println!("Frequencies:");
        println!("FRANCE  (+3): {:.3}", france_freq);
        println!("CHINA   (+2): {:.3}", china_freq);
        println!("BRAZIL  (+1): {:.3}", brazil_freq);
        println!("ALGERIA (+0): {:.3}", algeria_freq);
        println!("JAPAN   (-2): {:.3}", japan_freq);

        // Test relative ordering
        assert!(
            france_count > china_count,
            "FRANCE(+3) should appear more than CHINA(+2): {} vs {}",
            france_count,
            china_count
        );

        assert!(
            china_count > brazil_count,
            "CHINA(+2) should appear more than BRAZIL(+1): {} vs {}",
            china_count,
            brazil_count
        );

        assert!(
            brazil_count > algeria_count,
            "BRAZIL(+1) should appear more than ALGERIA(+0): {} vs {}",
            brazil_count,
            algeria_count
        );

        assert!(
            algeria_count > japan_count,
            "ALGERIA(+0) should appear more than JAPAN(-2): {} vs {}",
            algeria_count,
            japan_count
        );

        // Verify Japan still appears
        assert!(
            japan_count > 0,
            "JAPAN should still be sampled despite negative adjustment (got {})",
            japan_count
        );

        // Test for reasonable frequency ranges
        // With base_weight = 10, the weights would be:
        // FRANCE:  13 -> ~25%
        // CHINA:   12 -> ~23%
        // BRAZIL:  11 -> ~21%
        // ALGERIA: 10 -> ~19%
        // JAPAN:    8 -> ~15%

        let epsilon = 0.03; // Allow 3% deviation

        assert!(
            (france_freq - 0.25).abs() < epsilon,
            "FRANCE frequency {} should be close to 0.25",
            france_freq
        );

        assert!(
            (china_freq - 0.23).abs() < epsilon,
            "CHINA frequency {} should be close to 0.23",
            china_freq
        );

        assert!(
            (brazil_freq - 0.21).abs() < epsilon,
            "BRAZIL frequency {} should be close to 0.21",
            brazil_freq
        );

        assert!(
            (algeria_freq - 0.19).abs() < epsilon,
            "ALGERIA frequency {} should be close to 0.19",
            algeria_freq
        );

        assert!(
            (japan_freq - 0.15).abs() < epsilon,
            "JAPAN frequency {} should be close to 0.15",
            japan_freq
        );
    }
    #[test]
    fn can_handle_comments() {
        let input = r#"
BEGIN grammar
COUNT|2
N V T|1
N V N T|1
END grammar

BEGIN np
COUNT|1
packages|1
END np

BEGIN vp
COUNT|1
sleep|1
END vp

BEGIN terminators
COUNT|1
.|1
END terminators
"#;
        let mut parser = DistributionParser::new();
        parser.parse(Cursor::new(input)).unwrap();

        let mut rng = StdRng::seed_from_u64(42);
        let comment = parser.generate_comment(&mut rng).unwrap();

        // Verify comment structure
        assert!(comment.ends_with("."));
        assert!(comment.contains("packages"));
        assert!(comment.contains("sleep"));
    }

    #[test]
    fn can_properly_handle_bad_input() {
        let invalid_input = r#"
BEGIN test_invalid
COUNT|2
value1|not_a_number
value2|1
END test_invalid
"#;
        let mut parser = DistributionParser::new();
        assert!(matches!(
            parser.parse(Cursor::new(invalid_input)),
            Err(DistParserError::InvalidWeight(_))
        ));

        let duplicate_input =
            "BEGIN test\nCOUNT|1\nvalue|1\nEND test\nBEGIN test\nCOUNT|1\nvalue|1\nEND test";
        assert!(matches!(
            parser.parse(Cursor::new(duplicate_input)),
            Err(DistParserError::DuplicateDistribution(_))
        ));
    }

    #[test]
    fn can_parse_tpch_dist_file() {
        let input = DISTS_SEED;
        let mut parser = DistributionParser::new();
        assert!(parser.parse(Cursor::new(input)).is_ok());
        parser
            .distributions
            .iter()
            .for_each(|dist| println!("Distribution:\n {}", dist.1));
    }
}
