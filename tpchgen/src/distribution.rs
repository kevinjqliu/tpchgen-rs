use crate::random::RowRandomInt;
use std::{
    collections::HashMap,
    io::{self, BufRead},
    sync::LazyLock,
};

/// TPC-H distributions seed file.
pub(crate) const DISTS_SEED: &str = include_str!("dists.dss");

/// Distribution represents a weighted collection of string values from the TPC-H specification.
/// It provides methods to access values by index or randomly based on their weights.
#[derive(Debug, Clone)]
pub struct Distribution {
    name: String,
    values: Vec<String>,
    weights: Vec<i32>,
    distribution: Option<Vec<String>>,
    max_weight: i32,
}

impl Distribution {
    /// Creates a new Distribution with the given name and weighted values.
    pub fn new(name: String, distribution: Vec<(String, i32)>) -> Self {
        let mut weights = vec![0; distribution.len()];

        let mut running_weight = 0;
        let mut is_valid_distribution = true;

        // Process each value and its weight
        for (index, (_, weight)) in distribution.iter().enumerate() {
            running_weight += weight;
            weights[index] = running_weight;

            // A valid distribution requires all weights to be positive
            is_valid_distribution &= *weight > 0;
        }

        // Only create the full distribution array for valid distributions
        // "nations" is a special case that's not a valid distribution
        let (distribution_array, max_weight) = if is_valid_distribution {
            let max = weights[weights.len() - 1];
            let mut dist = vec![String::new(); max as usize];

            let mut index = 0;
            for (value, weight) in &distribution {
                for _ in 0..*weight {
                    dist[index] = value.clone();
                    index += 1;
                }
            }

            (Some(dist), max)
        } else {
            (None, -1)
        };

        let values = distribution.into_iter().map(|(tok, _)| tok).collect();

        Distribution {
            name,
            values,
            weights,
            distribution: distribution_array,
            max_weight,
        }
    }

    /// Returns the distribution name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets a value at the specified index.
    pub fn get_value(&self, index: usize) -> &str {
        &self.values[index]
    }

    /// Gets all values in this distribution.
    pub fn get_values(&self) -> &[String] {
        &self.values
    }

    /// Gets the cumulative weight at the specified index.
    pub fn get_weight(&self, index: usize) -> i32 {
        self.weights[index]
    }

    /// Gets the number of distinct values in this distribution.
    pub fn size(&self) -> usize {
        self.values.len()
    }

    /// Gets a random value from this distribution using the provided random number.
    pub fn random_value(&self, random: &mut RowRandomInt) -> &str {
        if let Some(dist) = &self.distribution {
            let random_value = random.next_int(0, self.max_weight - 1);
            return &dist[random_value as usize];
        }
        unreachable!("Cannot get random value from an invalid distribution")
    }
}

/// DistributionLoader provides functionality to load TPC-H distributions from a text format.
pub struct DistributionLoader;

impl DistributionLoader {
    /// Loads distributions from a stream of lines.
    ///
    /// The format is expected to follow the TPC-H specification format where:
    /// - Lines starting with `"#"` are comments
    /// - Distributions start with `"BEGIN <name>"`
    /// - Distribution entries are formatted as `"value|weight"`
    /// - Distributions end with `"END"`
    pub fn load_distributions<I>(lines: I) -> io::Result<HashMap<String, Distribution>>
    where
        I: Iterator<Item = io::Result<String>>,
    {
        let filtered_lines = lines.filter_map(|line_result| {
            line_result.ok().and_then(|line| {
                let trimmed = line.trim();
                if !trimmed.is_empty() && !trimmed.starts_with('#') {
                    Some(trimmed.to_string())
                } else {
                    None
                }
            })
        });

        Self::load_distributions_from_filtered_lines(filtered_lines)
    }

    /// Internal method to load distributions from pre-filtered lines.
    fn load_distributions_from_filtered_lines<I>(
        lines: I,
    ) -> io::Result<HashMap<String, Distribution>>
    where
        I: Iterator<Item = String>,
    {
        let mut distributions = HashMap::new();
        let mut lines_iter = lines.peekable();

        while let Some(line) = lines_iter.next() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case("BEGIN") {
                let name = parts[1].to_string();
                let distribution = Self::load_distribution(&mut lines_iter, &name)?;
                distributions.insert(name, distribution);
            }
        }

        Ok(distributions)
    }

    /// Loads a single distribution until its END marker.
    fn load_distribution<I>(
        lines: &mut std::iter::Peekable<I>,
        name: &str,
    ) -> io::Result<Distribution>
    where
        I: Iterator<Item = String>,
    {
        // (Token, Weight) pairs within a distribution.
        let mut members: Vec<(String, i32)> = Vec::new();
        let mut _count = -1;

        for line in lines.by_ref() {
            if Self::is_end(&line) {
                let distribution = Distribution::new(name.to_string(), members);
                return Ok(distribution);
            }

            let parts: Vec<&str> = line.split("|").collect::<Vec<_>>();
            if parts.len() < 2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid distribution line format: {}", line),
                ));
            }

            let value = parts[0];
            let weight = match parts[1].trim().parse::<i32>() {
                Ok(w) => w,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid distribution {}: invalid weight on line {}",
                            name, line
                        ),
                    ));
                }
            };

            if value.eq_ignore_ascii_case("count") {
                _count = weight;
            } else {
                members.push((value.to_string(), weight));
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid distribution {}: no end statement", name),
        ))
    }

    /// Checks if a line is an END marker.
    fn is_end(line: &str) -> bool {
        let parts: Vec<&str> = line.split_whitespace().collect();
        parts.first().is_some_and(|p| p.eq_ignore_ascii_case("END"))
    }
}

/// Static global instance of the default distributions.
///
/// Initialized once on first access.
static DEFAULT_DISTRIBUTIONS: LazyLock<Distributions> =
    LazyLock::new(|| Distributions::try_load_defualt().unwrap());

/// Distributions wraps all TPC-H distributions and provides methods to access them.
#[derive(Debug, Clone)]
pub struct Distributions {
    articles: Distribution,
    adjectives: Distribution,
    adverbs: Distribution,
    auxiliaries: Distribution,
    grammar: Distribution,
    category: Distribution,
    market_segments: Distribution,
    nations: Distribution,
    noun_phrase: Distribution,
    nouns: Distribution,
    order_priority: Distribution,
    part_colors: Distribution,
    part_containers: Distribution,
    part_types: Distribution,
    prepositions: Distribution,
    regions: Distribution,
    return_flags: Distribution,
    ship_instructions: Distribution,
    ship_modes: Distribution,
    terminators: Distribution,
    verb_phrase: Distribution,
    verbs: Distribution,
}

impl Distributions {
    pub fn try_load_defualt() -> io::Result<Self> {
        let cursor = io::Cursor::new(DISTS_SEED);
        let lines = cursor.lines();
        let mut distributions = DistributionLoader::load_distributions(lines).unwrap();

        let remove_dist = &mut |key: &str| {
            distributions.remove(key).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Missing distribution: {key}"),
                )
            })
        };

        let articles = remove_dist("articles")?;
        let adjectives = remove_dist("adjectives")?;
        let adverbs = remove_dist("adverbs")?;
        let auxiliaries = remove_dist("auxillaries")?; // P.S: The correct spelling is `auxiliaries` which is what we use.
        let grammar = remove_dist("grammar")?;
        let category = remove_dist("category")?;
        let market_segments = remove_dist("msegmnt")?;
        let nations = remove_dist("nations")?;
        let noun_phrase = remove_dist("np")?;
        let nouns = remove_dist("nouns")?;
        let order_priority = remove_dist("o_oprio")?;
        let part_colors = remove_dist("colors")?;
        let part_containers = remove_dist("p_cntr")?;
        let part_types = remove_dist("p_types")?;
        let prepositions = remove_dist("prepositions")?;
        let regions = remove_dist("regions")?;
        let return_flags = remove_dist("rflag")?;
        let ship_instructions = remove_dist("instruct")?;
        let ship_modes = remove_dist("smode")?;
        let terminators = remove_dist("terminators")?;
        let verb_phrase = remove_dist("vp")?;
        let verbs = remove_dist("verbs")?;

        // currently unused distributions
        remove_dist("nations2")?;
        remove_dist("Q13a")?;
        remove_dist("Q13b")?;
        remove_dist("p_names")?;

        // Ensure that all distributions have been removed.
        if !distributions.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Internal Error: Unused distributions: {:?}",
                    distributions.keys().collect::<Vec<_>>()
                ),
            ));
        }

        let new_self = Distributions {
            articles,
            adjectives,
            adverbs,
            auxiliaries,
            grammar,
            category,
            market_segments,
            nations,
            noun_phrase,
            nouns,
            order_priority,
            part_colors,
            part_containers,
            part_types,
            prepositions,
            regions,
            return_flags,
            ship_instructions,
            ship_modes,
            terminators,
            verb_phrase,
            verbs,
        };

        Ok(new_self)
    }

    /// Returns a static reference to the default distributions.
    pub fn static_default() -> &'static Distributions {
        &DEFAULT_DISTRIBUTIONS
    }

    /// Returns the `adjectives` distribution.
    pub fn adjectives(&self) -> &Distribution {
        &self.adjectives
    }

    /// Returns the `adverbs` distribution.
    pub fn adverbs(&self) -> &Distribution {
        &self.adverbs
    }

    /// Returns the `articles` distribution.
    pub fn articles(&self) -> &Distribution {
        &self.articles
    }

    /// Returns the `auxillaries` distribution.
    ///
    /// P.S: The correct spelling is `auxiliaries` which is what we use.
    pub fn auxiliaries(&self) -> &Distribution {
        &self.auxiliaries
    }

    /// Returns the `grammar` distribution.
    pub fn grammar(&self) -> &Distribution {
        &self.grammar
    }

    /// Returns the `category` distribution.
    pub fn category(&self) -> &Distribution {
        &self.category
    }

    /// Returns the `msegmnt` distribution.
    pub fn market_segments(&self) -> &Distribution {
        &self.market_segments
    }

    /// Returns the `nations` distribution.
    pub fn nations(&self) -> &Distribution {
        &self.nations
    }

    /// Returns the `noun_phrases` distribution.
    pub fn noun_phrase(&self) -> &Distribution {
        &self.noun_phrase
    }

    /// Returns the `nouns` distribution.
    pub fn nouns(&self) -> &Distribution {
        &self.nouns
    }

    /// Returns the `orders_priority` distribution.
    pub fn order_priority(&self) -> &Distribution {
        &self.order_priority
    }

    /// Returns the `part_colors` distribution.
    pub fn part_colors(&self) -> &Distribution {
        &self.part_colors
    }

    /// Returns the `part_containers` distribution.
    pub fn part_containers(&self) -> &Distribution {
        &self.part_containers
    }

    /// Returns the `part_types` distribution.
    pub fn part_types(&self) -> &Distribution {
        &self.part_types
    }

    /// Returns the `prepositions` distribution.
    pub fn prepositions(&self) -> &Distribution {
        &self.prepositions
    }

    /// Returns the `regions` distribution.
    pub fn regions(&self) -> &Distribution {
        &self.regions
    }

    /// Returns the `return_flags` distribution.
    pub fn return_flags(&self) -> &Distribution {
        &self.return_flags
    }

    /// Returns the `ship_instructions` distribution.
    pub fn ship_instructions(&self) -> &Distribution {
        &self.ship_instructions
    }

    /// Returns the `ship_modes` distribution.
    pub fn ship_modes(&self) -> &Distribution {
        &self.ship_modes
    }

    /// Returns the `terminators` distribution.
    pub fn terminators(&self) -> &Distribution {
        &self.terminators
    }

    // Returns the `verb_phrases` distribution.
    pub fn verb_phrase(&self) -> &Distribution {
        &self.verb_phrase
    }

    /// Returns the `verbs` distribution.
    pub fn verbs(&self) -> &Distribution {
        &self.verbs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_load_empty() {
        let input = "";
        let cursor = Cursor::new(input);
        let lines = cursor.lines();
        let distributions = DistributionLoader::load_distributions(lines).unwrap();
        assert!(distributions.is_empty());
    }

    #[test]
    fn test_load_simple_distribution() {
        let input = "
            # Comment line
            BEGIN test
            value1|10
            value2|20
            END
        ";
        let cursor = Cursor::new(input);
        let lines = cursor.lines();
        let distributions = DistributionLoader::load_distributions(lines).unwrap();

        assert_eq!(distributions.len(), 1);
        assert!(distributions.contains_key("test"));

        let test_dist = distributions.get("test").unwrap();
        assert_eq!(test_dist.size(), 2);
        assert_eq!(test_dist.get_value(0), "value1");
        assert_eq!(test_dist.get_value(1), "value2");
        assert_eq!(test_dist.get_weight(0), 10);
        assert_eq!(test_dist.get_weight(1), 30); // Cumulative weight
    }

    #[test]
    fn test_load_multiple_distributions() {
        let input = "
            BEGIN first
            a|5
            b|10
            END

            BEGIN second
            x|2
            y|3
            z|4
            END
        ";
        let cursor = Cursor::new(input);
        let lines = cursor.lines();
        let distributions = DistributionLoader::load_distributions(lines).unwrap();

        assert_eq!(distributions.len(), 2);
        assert!(distributions.contains_key("first"));
        assert!(distributions.contains_key("second"));

        let first_dist = distributions.get("first").unwrap();
        assert_eq!(first_dist.size(), 2);

        let second_dist = distributions.get("second").unwrap();
        assert_eq!(second_dist.size(), 3);
    }

    #[test]
    fn test_error_on_invalid_weight() {
        let input = "
            BEGIN test
            value|invalid
            END
        ";
        let cursor = Cursor::new(input);
        let lines = cursor.lines();
        let result = DistributionLoader::load_distributions(lines);
        assert!(result.is_err());
    }

    #[test]
    fn test_error_on_missing_end() {
        let input = "
            BEGIN test
            value|10
        ";
        let cursor = Cursor::new(input);
        let lines = cursor.lines();
        let result = DistributionLoader::load_distributions(lines);
        assert!(result.is_err());
    }

    #[test]
    fn test_with_default_seeds_file() {
        let expected_distributions = vec![
            "category",
            "p_cntr",
            "instruct",
            "msegmnt",
            "p_names",
            "nations",
            "nations2",
            "regions",
            "o_oprio",
            "regions",
            "rflag",
            "smode",
            "p_types",
            "colors",
            "articles",
            "nouns",
            "verbs",
            "adjectives",
            "adverbs",
            "auxillaries",
            "prepositions",
            "terminators",
            "grammar",
            "np",
            "vp",
            "Q13a",
            "Q13b",
        ];

        let cursor = Cursor::new(DISTS_SEED);
        let lines = cursor.lines();
        let distributions = DistributionLoader::load_distributions(lines).unwrap();
        assert_eq!(distributions.len(), 26);

        for name in expected_distributions {
            assert!(
                distributions.contains_key(name),
                "missing distribution: {}",
                name
            );
        }
    }
}
