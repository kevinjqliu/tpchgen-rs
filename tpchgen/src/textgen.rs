/*
use std::collections::HashMap;

use crate::{distribution::Distribution, rng::TpchRng};

// Trait for progress monitoring
pub trait TextGenerationProgressMonitor {
    fn update_progress(&self, progress: f64);
}

// Default progress monitor implementation
impl TextGenerationProgressMonitor for Box<dyn Fn(f64)> {
    fn update_progress(&self, progress: f64) {
        self(progress);
    }
}

// Indexed distribution for simple word lists
struct IndexedDistribution {
    random_table: Vec<String>,
}

impl IndexedDistribution {
    fn new(distribution: &Distribution) -> Self {
        let max_weight = distribution.get_weight(distribution.size() - 1).unwrap();
        let mut random_table = Vec::with_capacity(max_weight as usize);

        let mut value_index = 0;
        for i in 0..max_weight {
            if i >= distribution.get_weight(value_index).unwrap() {
                value_index += 1;
            }
            random_table.push(distribution.get_value(value_index).unwrap().to_string());
        }

        Self { random_table }
    }

    fn random_value(&self, random: &mut TpchRng) -> &str {
        let random_index = random.next_int(0, (self.random_table.len() - 1) as i32) as usize;
        &self.random_table[random_index]
    }
}

// Parsed distribution for grammar rules
struct ParsedDistribution {
    parsed_distribution: Vec<Vec<char>>,
    bonus_text: Vec<String>,
    random_table: Vec<usize>,
}

impl ParsedDistribution {
    fn new(distribution: &Distribution) -> Self {
        let size = distribution.size();
        let mut parsed_distribution = Vec::with_capacity(size);
        let mut bonus_text = Vec::with_capacity(size);

        for i in 0..size {
            let tokens: Vec<&str> = distribution
                .get_value(i)
                .unwrap()
                .split_whitespace()
                .collect();

            let mut chars = Vec::with_capacity(tokens.len());
            for token in &tokens {
                let mut chars_iter = token.chars();
                if let Some(first_char) = chars_iter.next() {
                    chars.push(first_char);
                    bonus_text.push(chars_iter.collect());
                }
            }
            parsed_distribution.push(chars);
        }

        let max_weight = distribution.get_weight(size - 1).unwrap();
        let mut random_table = Vec::with_capacity(max_weight as usize);

        let mut value_index = 0;
        for i in 0..max_weight {
            if i >= distribution.get_weight(value_index).unwrap() {
                value_index += 1;
            }
            random_table.push(value_index);
        }

        Self {
            parsed_distribution,
            bonus_text,
            random_table,
        }
    }

    fn get_random_index(&self, random: &mut TpchRng) -> usize {
        let random_index = random.next_int(0, (self.random_table.len() - 1) as i32) as usize;
        self.random_table[random_index]
    }

    fn get_tokens(&self, index: usize) -> &[char] {
        &self.parsed_distribution[index]
    }

    fn get_bonus_text(&self, index: usize) -> &str {
        &self.bonus_text[index]
    }
}

pub struct TextPoolGenerator {
    size: usize,

    grammars: ParsedDistribution,
    noun_phrases: ParsedDistribution,
    verb_phrases: ParsedDistribution,

    prepositions: IndexedDistribution,
    terminators: IndexedDistribution,
    adverbs: IndexedDistribution,
    verbs: IndexedDistribution,
    auxiliaries: IndexedDistribution,
    articles: IndexedDistribution,
    adjectives: IndexedDistribution,
    nouns: IndexedDistribution,
}

const MAX_SENTENCE_LENGTH: usize = 256;

impl TextPoolGenerator {
    pub fn new(size: usize, distributions: &HashMap<String, Distribution>) -> Self {
        Self::new_with_monitor(size, distributions)
    }

    pub fn new_with_monitor(size: usize, distributions: &HashMap<String, Distribution>) -> Self {
        Self {
            size,
            grammars: ParsedDistribution::new(
                &distributions
                    .get("grammar")
                    .expect("expected key 'grammar'"),
            ),
            noun_phrases: ParsedDistribution::new(
                &distributions.get("np").expect("expected key 'np'"),
            ),
            verb_phrases: ParsedDistribution::new(
                &distributions.get("vp").expect("expected key 'vp'"),
            ),
            prepositions: IndexedDistribution::new(&distributions.get("prepositions").unwrap()),
            terminators: IndexedDistribution::new(&distributions.get("terminators").unwrap()),
            adverbs: IndexedDistribution::new(&distributions.get("adverbs").unwrap()),
            verbs: IndexedDistribution::new(&distributions.get("verbs").unwrap()),
            auxiliaries: IndexedDistribution::new(&distributions.get("auxillaries").unwrap()),
            articles: IndexedDistribution::new(&distributions.get("articles").unwrap()),
            adjectives: IndexedDistribution::new(&distributions.get("adjectives").unwrap()),
            nouns: IndexedDistribution::new(&distributions.get("nouns").unwrap()),
        }
    }

    pub fn generate(&self) -> String {
        let mut output = String::with_capacity(self.size + MAX_SENTENCE_LENGTH);
        let mut random = TpchRng::new(933588178, i32::MAX as i64);

        while output.len() < self.size {
            self.generate_sentence(&mut output, &mut random);
        }

        output.truncate(self.size);
        output
    }

    fn generate_sentence(&self, builder: &mut String, random: &mut TpchRng) {
        let index = self.grammars.get_random_index(random);

        for &token in self.grammars.get_tokens(index) {
            match token {
                'V' => self.generate_verb_phrase(builder, random),
                'N' => self.generate_noun_phrase(builder, random),
                'P' => {
                    let preposition = self.prepositions.random_value(random);
                    builder.push_str(preposition);
                    builder.push_str(" the ");
                    self.generate_noun_phrase(builder, random);
                }
                'T' => {
                    // Trim trailing space and add terminator
                    if builder.ends_with(' ') {
                        builder.pop();
                    }
                    let terminator = self.terminators.random_value(random);
                    builder.push_str(terminator);
                }
                _ => panic!("Unknown token '{}'", token),
            }

            if !builder.ends_with(' ') {
                builder.push(' ');
            }
        }
    }

    fn generate_verb_phrase(&self, builder: &mut String, random: &mut TpchRng) {
        let index = self.verb_phrases.get_random_index(random);

        for &token in self.verb_phrases.get_tokens(index) {
            match token {
                'D' => builder.push_str(self.adverbs.random_value(random)),
                'V' => builder.push_str(self.verbs.random_value(random)),
                'X' => builder.push_str(self.auxiliaries.random_value(random)),
                _ => panic!("Unknown token '{}'", token),
            }

            builder.push_str(self.verb_phrases.get_bonus_text(index));
            builder.push(' ');
        }
    }

    fn generate_noun_phrase(&self, builder: &mut String, random: &mut TpchRng) {
        let index = self.noun_phrases.get_random_index(random);

        for &token in self.noun_phrases.get_tokens(index) {
            match token {
                'A' => builder.push_str(self.articles.random_value(random)),
                'J' => builder.push_str(self.adjectives.random_value(random)),
                'D' => builder.push_str(self.adverbs.random_value(random)),
                'N' => builder.push_str(self.nouns.random_value(random)),
                _ => panic!("Unknown token '{}'", token),
            }

            builder.push_str(self.noun_phrases.get_bonus_text(index));
            builder.push(' ');
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::Cursor,
        sync::{atomic::AtomicUsize, Arc},
    };

    use crate::{
        distribution::{self, Distribution, DistributionParser, DISTS_SEED},
        rng::TpchRng,
        textgen::TextPoolGenerator,
    };

    fn create_mock_distributions() -> HashMap<String, Distribution> {
        let input = DISTS_SEED;
        let mut parser = DistributionParser::new();
        assert!(parser.parse(Cursor::new(input)).is_ok());
        parser.distributions().clone()
    }
    #[test]
    fn test_generator_initialization() {
        let distributions = create_mock_distributions();
        let generator = TextPoolGenerator::new(100, &distributions);
        assert!(generator.size == 100);
    }

    #[test]
    fn test_generated_text_length() {
        let distributions = create_mock_distributions();
        let generator = TextPoolGenerator::new(100, &distributions);
        let generated = generator.generate();
        assert_eq!(generated.len(), 100);
    }

    #[test]
    fn test_random_int_distribution() {
        let mut rng = TpchRng::new(933588178, i32::MAX as i64);
        let mut counts = vec![0; 10];

        for _ in 0..1000 {
            let val = rng.next_int(0, 9);
            assert!(val >= 0 && val <= 9);
            counts[val as usize] += 1;
        }

        // Check that we got a reasonable distribution
        for count in counts {
            assert!(count > 0); // Each number should appear at least once
        }
    }

    #[test]
    fn test_generate_sentence_components() {
        let distributions = create_mock_distributions();
        let generator = TextPoolGenerator::new(1000, &distributions);
        let text = generator.generate();

        // Check for basic sentence components
        assert!(
            text.contains(".") || text.contains("!"),
            "Text should contain terminators"
        );
        assert!(
            text.contains("the") || text.contains("a"),
            "Text should contain articles"
        );
    }

    #[test]
    fn test_grammar_token() {
        let mut distributions = create_mock_distributions();

        let generator = TextPoolGenerator::new(100, &distributions);
        generator.generate();
    }

    #[test]
    fn test_consistent_output() {
        let distributions = create_mock_distributions();
        let generator = TextPoolGenerator::new(100, &distributions);

        let first_output = generator.generate();
        let second_output = generator.generate();

        // With the same seed, should generate the same text
        assert_eq!(first_output, second_output);
    }

    #[test]
    fn test_different_sizes() {
        let distributions = create_mock_distributions();
        let sizes = [50, 100, 200];

        for &size in &sizes {
            let generator = TextPoolGenerator::new(size, &distributions);
            let output = generator.generate();
            assert_eq!(output.len(), size);
        }
    }

    #[test]
    fn test_bonus_text_handling() {
        let mut distributions = create_mock_distributions();

        let generator = TextPoolGenerator::new(100, &distributions);
        let output = generator.generate();

        assert!(
            output.contains(", "),
            "Output should contain bonus text (comma)"
        );
    }
}

*/
