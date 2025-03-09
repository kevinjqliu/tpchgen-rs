use core::fmt;
use std::vec::IntoIter;

use crate::distribution::Distribution;
use crate::distribution::Distributions;
use crate::random::RandomAlphaNumeric;
use crate::random::RandomPhoneNumber;
use crate::random::RowRandomInt;
use crate::text::TextPool;
use std::sync::Arc;

use crate::dates::GenerateUtils;
use crate::random::{RandomBoundedInt, RandomString, RandomStringSequence, RandomText};

/// Generator for Nation table data
pub struct NationGenerator {
    distributions: Distributions,
    text_pool: TextPool,
}

impl NationGenerator {
    /// Creates a new NationGenerator with default distributions and text pool
    pub fn new() -> Self {
        Self::new_with_distributions_and_text_pool(Distributions::default(), TextPool::default())
    }

    /// Creates a NationGenerator with the specified distributions and text pool
    pub fn new_with_distributions_and_text_pool(
        distributions: Distributions,
        text_pool: TextPool,
    ) -> Self {
        NationGenerator {
            distributions,
            text_pool,
        }
    }

    /// Returns an iterator over the nation rows
    pub fn iter(&self) -> NationGeneratorIterator {
        NationGeneratorIterator::new(self.distributions.nations(), &self.text_pool)
    }
}

impl IntoIterator for NationGenerator {
    type Item = Nation;
    type IntoIter = NationGeneratorIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// The NATION table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nation {
    /// Primary key (0-24)
    pub n_nationkey: i64,
    /// Nation name
    pub n_name: String,
    /// Foreign key to REGION
    pub n_regionkey: i64,
    /// Variable length comment
    pub n_comment: String,
}

impl fmt::Display for Nation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}, {}, {}, {}",
            self.n_nationkey, self.n_name, self.n_regionkey, self.n_comment
        )
    }
}

impl Nation {
    /// Create a new `nation` record with the specified values.
    pub fn new(n_nationkey: i64, n_name: &str, n_regionkey: i64, n_comment: &str) -> Self {
        Nation {
            n_nationkey,
            n_name: n_name.to_string(),
            n_regionkey,
            n_comment: n_comment.to_string(),
        }
    }
}

/// Iterator that generates Nation rows
pub struct NationGeneratorIterator {
    nations: Distribution,
    comment_random: RandomText,
    index: usize,
}

impl NationGeneratorIterator {
    const COMMENT_AVERAGE_LENGTH: i32 = 72;

    fn new(nations: &Distribution, text_pool: &TextPool) -> Self {
        NationGeneratorIterator {
            nations: nations.clone(),
            comment_random: RandomText::new(
                606179079,
                text_pool,
                Self::COMMENT_AVERAGE_LENGTH as f64,
            ),
            index: 0,
        }
    }
}

impl Iterator for NationGeneratorIterator {
    type Item = Nation;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.nations.size() {
            return None;
        }

        let nation = Nation {
            // n_nationkey
            n_nationkey: self.index as i64,
            // n_name
            n_name: self.nations.get_value(self.index).to_string(),
            // n_regionkey
            n_regionkey: self.nations.get_weight(self.index) as i64,
            // n_comment
            n_comment: self.comment_random.next_value(),
        };

        self.comment_random.row_finished();
        self.index += 1;

        Some(nation)
    }
}

/// The REGION table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    /// Primary key (0-4)
    pub r_regionkey: i64,
    /// Region name (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)
    pub r_name: String,
    /// Variable length comment
    pub r_comment: String,
}

impl Region {
    /// Creates a new `region` record with the specified values.
    pub fn new(r_regionkey: i64, r_name: &str, r_comment: &str) -> Self {
        Region {
            r_regionkey,
            r_name: r_name.to_string(),
            r_comment: r_comment.to_string(),
        }
    }
}

/// Generator for Region table data
pub struct RegionGenerator {
    distributions: Distributions,
    text_pool: TextPool,
}

impl RegionGenerator {
    /// Creates a new RegionGenerator with default distributions and text pool
    pub fn new() -> Self {
        Self::new_with_distributions_and_text_pool(Distributions::default(), TextPool::default())
    }

    /// Creates a RegionGenerator with the specified distributions and text pool
    pub fn new_with_distributions_and_text_pool(
        distributions: Distributions,
        text_pool: TextPool,
    ) -> Self {
        RegionGenerator {
            distributions,
            text_pool,
        }
    }

    /// Returns an iterator over the region rows
    pub fn iter(&self) -> RegionGeneratorIterator {
        RegionGeneratorIterator::new(self.distributions.regions().clone(), &self.text_pool)
    }
}

impl IntoIterator for RegionGenerator {
    type Item = Region;
    type IntoIter = RegionGeneratorIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Region rows
pub struct RegionGeneratorIterator {
    regions: Distribution,
    comment_random: RandomText,
    index: usize,
}

impl RegionGeneratorIterator {
    const COMMENT_AVERAGE_LENGTH: i32 = 72;

    fn new(regions: Distribution, text_pool: &TextPool) -> Self {
        RegionGeneratorIterator {
            regions,
            comment_random: RandomText::new(
                1500869201,
                text_pool,
                Self::COMMENT_AVERAGE_LENGTH as f64,
            ),
            index: 0,
        }
    }
}

impl Iterator for RegionGeneratorIterator {
    type Item = Region;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.regions.size() {
            return None;
        }

        let region = Region {
            r_regionkey: self.index as i64,
            r_name: self.regions.get_value(self.index).to_string(),
            r_comment: self.comment_random.next_value(),
        };

        self.comment_random.row_finished();
        self.index += 1;

        Some(region)
    }
}

/// The PART table
#[derive(Debug, Clone, PartialEq)]
pub struct Part {
    /// Primary key
    pub p_partkey: i64,
    /// Part name
    pub p_name: String,
    /// Part manufacturer
    pub p_mfgr: String,
    /// Part brand
    pub p_brand: String,
    /// Part type
    pub p_type: String,
    /// Part size
    pub p_size: i32,
    /// Part container
    pub p_container: String,
    /// Part retail price
    pub p_retailprice: f64,
    /// Variable length comment
    pub p_comment: String,
}

impl fmt::Display for Part {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}, {}, {}, {},{}, {}, {}, {:.2}, {}",
            self.p_partkey,
            self.p_name,
            self.p_mfgr,
            self.p_brand,
            self.p_type,
            self.p_size,
            self.p_container,
            self.p_retailprice,
            self.p_comment
        )
    }
}

/// Generator for Part table data
pub struct PartGenerator {
    scale_factor: f64,
    part: i32,
    part_count: i32,
    distributions: Distributions,
    text_pool: Arc<TextPool>,
}

impl PartGenerator {
    /// Base scale for part generation
    const SCALE_BASE: i32 = 200_000;

    // Constants for part generation
    const NAME_WORDS: i32 = 5;
    const MANUFACTURER_MIN: i32 = 1;
    const MANUFACTURER_MAX: i32 = 5;
    const BRAND_MIN: i32 = 1;
    const BRAND_MAX: i32 = 5;
    const SIZE_MIN: i32 = 1;
    const SIZE_MAX: i32 = 50;
    const COMMENT_AVERAGE_LENGTH: i32 = 14;
    /// Creates a new PartGenerator with the given scale factor
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> Self {
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            part,
            part_count,
            Distributions::default(),
            Arc::new(TextPool::default()),
        )
    }

    /// Creates a PartGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        distributions: Distributions,
        text_pool: Arc<TextPool>,
    ) -> Self {
        PartGenerator {
            scale_factor,
            part,
            part_count,
            distributions,
            text_pool,
        }
    }

    /// Returns an iterator over the part rows
    pub fn iter(&self) -> PartGeneratorIterator {
        PartGeneratorIterator::new(
            &self.distributions,
            self.text_pool.clone(),
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
            GenerateUtils::calculate_row_count(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
        )
    }
}

impl IntoIterator for PartGenerator {
    type Item = Part;
    type IntoIter = PartGeneratorIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Part rows
pub struct PartGeneratorIterator {
    name_random: RandomStringSequence,
    manufacturer_random: RandomBoundedInt,
    brand_random: RandomBoundedInt,
    type_random: RandomString,
    size_random: RandomBoundedInt,
    container_random: RandomString,
    comment_random: RandomText,

    start_index: i64,
    row_count: i64,
    index: i64,
}

impl PartGeneratorIterator {
    fn new(
        distributions: &Distributions,
        text_pool: Arc<TextPool>,
        start_index: i64,
        row_count: i64,
    ) -> Self {
        let mut name_random = RandomStringSequence::new(
            709314158,
            PartGenerator::NAME_WORDS,
            distributions.part_colors(),
        );
        let mut manufacturer_random = RandomBoundedInt::new(
            1,
            PartGenerator::MANUFACTURER_MIN,
            PartGenerator::MANUFACTURER_MAX,
        );
        let mut brand_random =
            RandomBoundedInt::new(46831694, PartGenerator::BRAND_MIN, PartGenerator::BRAND_MAX);
        let mut type_random = RandomString::new(1841581359, distributions.part_types());
        let mut size_random =
            RandomBoundedInt::new(1193163244, PartGenerator::SIZE_MIN, PartGenerator::SIZE_MAX);
        let mut container_random = RandomString::new(727633698, distributions.part_containers());
        let mut comment_random = RandomText::new(
            804159733,
            &text_pool,
            PartGenerator::COMMENT_AVERAGE_LENGTH as f64,
        );

        // Advance all generators to the starting position
        name_random.advance_rows(start_index);
        manufacturer_random.advance_rows(start_index);
        brand_random.advance_rows(start_index);
        type_random.advance_rows(start_index);
        size_random.advance_rows(start_index);
        container_random.advance_rows(start_index);
        comment_random.advance_rows(start_index);

        PartGeneratorIterator {
            name_random,
            manufacturer_random,
            brand_random,
            type_random,
            size_random,
            container_random,
            comment_random,
            start_index,
            row_count,
            index: 0,
        }
    }

    /// Creates a part with the given key
    fn make_part(&mut self, part_key: i64) -> Part {
        let name = self.name_random.next_value();

        let manufacturer = self.manufacturer_random.next_value();
        let brand = manufacturer * 10 + self.brand_random.next_value();

        let part = Part {
            p_partkey: part_key,
            p_name: name,
            p_mfgr: format!("Manufacturer#{}", manufacturer),
            p_brand: format!("Brand#{}", brand),
            p_type: self.type_random.next_value(),
            p_size: self.size_random.next_value(),
            p_container: self.container_random.next_value(),
            p_retailprice: Self::calculate_part_price(part_key) as f64 / 100.0,
            p_comment: self.comment_random.next_value(),
        };

        part
    }

    /// Calculates the price for a part
    pub fn calculate_part_price(part_key: i64) -> i64 {
        let mut price = 90000;

        // limit contribution to $200
        price += (part_key / 10) % 20001;
        price += (part_key % 1000) * 100;

        price
    }
}

impl Iterator for PartGeneratorIterator {
    type Item = Part;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let part = self.make_part(self.start_index + self.index + 1);

        self.name_random.row_finished();
        self.manufacturer_random.row_finished();
        self.brand_random.row_finished();
        self.type_random.row_finished();
        self.size_random.row_finished();
        self.container_random.row_finished();
        self.comment_random.row_finished();

        self.index += 1;

        Some(part)
    }
}

/// Records for the SUPPLIER table.
#[derive(Debug, Clone, PartialEq)]
pub struct Supplier {
    /// Primary key
    pub s_suppkey: i64,
    /// Supplier name
    pub s_name: String,
    /// Supplier address
    pub s_address: String,
    /// Foreign key to NATION
    pub s_nationkey: i64,
    /// Supplier phone number
    pub s_phone: String,
    /// Supplier account balance
    pub s_acctbal: f64,
    /// Variable length comment
    pub s_comment: String,
}

impl fmt::Display for Supplier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}, {}, {}, {}, {}, {:.2}, {}",
            self.s_suppkey,
            self.s_name,
            self.s_address,
            self.s_nationkey,
            self.s_phone,
            self.s_acctbal,
            self.s_comment
        )
    }
}

/// Generator for Supplier table data
pub struct SupplierGenerator {
    scale_factor: f64,
    part: i32,
    part_count: i32,
    distributions: Distributions,
    text_pool: Arc<TextPool>,
}

impl SupplierGenerator {
    /// Base scale for supplier generation
    const SCALE_BASE: i32 = 10_000;

    // Constants for supplier generation
    const ACCOUNT_BALANCE_MIN: i32 = -99999;
    const ACCOUNT_BALANCE_MAX: i32 = 999999;
    const ADDRESS_AVERAGE_LENGTH: i32 = 25;
    const COMMENT_AVERAGE_LENGTH: i32 = 63;

    // Better Business Bureau comment constants
    pub const BBB_BASE_TEXT: &str = "Customer ";
    pub const BBB_COMPLAINT_TEXT: &str = "Complaints";
    pub const BBB_RECOMMEND_TEXT: &str = "Recommends";
    pub const BBB_COMMENT_LENGTH: usize =
        Self::BBB_BASE_TEXT.len() + Self::BBB_COMPLAINT_TEXT.len();
    pub const BBB_COMMENTS_PER_SCALE_BASE: i32 = 10;
    pub const BBB_COMPLAINT_PERCENT: i32 = 50;

    /// Creates a new SupplierGenerator with the given scale factor
    pub fn new(scale_factor: f64, part: i32, part_count: i32) -> Self {
        Self::new_with_distributions_and_text_pool(
            scale_factor,
            part,
            part_count,
            Distributions::default(),
            Arc::new(TextPool::default()),
        )
    }

    /// Creates a SupplierGenerator with specified distributions and text pool
    pub fn new_with_distributions_and_text_pool(
        scale_factor: f64,
        part: i32,
        part_count: i32,
        distributions: Distributions,
        text_pool: Arc<TextPool>,
    ) -> Self {
        SupplierGenerator {
            scale_factor,
            part,
            part_count,
            distributions,
            text_pool,
        }
    }

    /// Returns an iterator over the supplier rows
    pub fn iter(&self) -> SupplierGeneratorIterator {
        SupplierGeneratorIterator::new(
            &self.distributions,
            self.text_pool.clone(),
            GenerateUtils::calculate_start_index(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
            GenerateUtils::calculate_row_count(
                Self::SCALE_BASE,
                self.scale_factor,
                self.part,
                self.part_count,
            ),
        )
    }
}

impl IntoIterator for SupplierGenerator {
    type Item = Supplier;
    type IntoIter = SupplierGeneratorIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator that generates Supplier rows
pub struct SupplierGeneratorIterator {
    address_random: RandomAlphaNumeric,
    nation_key_random: RandomBoundedInt,
    phone_random: RandomPhoneNumber,
    account_balance_random: RandomBoundedInt,
    comment_random: RandomText,
    bbb_comment_random: RandomBoundedInt,
    bbb_junk_random: RowRandomInt,
    bbb_offset_random: RowRandomInt,
    bbb_type_random: RandomBoundedInt,

    start_index: i64,
    row_count: i64,
    index: i64,
}

impl SupplierGeneratorIterator {
    fn new(
        distributions: &Distributions,
        text_pool: Arc<TextPool>,
        start_index: i64,
        row_count: i64,
    ) -> Self {
        let mut address_random =
            RandomAlphaNumeric::new(706178559, SupplierGenerator::ADDRESS_AVERAGE_LENGTH);
        let mut nation_key_random =
            RandomBoundedInt::new(110356601, 0, (distributions.nations().size() - 1) as i32);
        let mut phone_random = RandomPhoneNumber::new(884434366);
        let mut account_balance_random = RandomBoundedInt::new(
            962338209,
            SupplierGenerator::ACCOUNT_BALANCE_MIN,
            SupplierGenerator::ACCOUNT_BALANCE_MAX,
        );
        let mut comment_random = RandomText::new(
            1341315363,
            &text_pool,
            SupplierGenerator::COMMENT_AVERAGE_LENGTH as f64,
        );
        let mut bbb_comment_random =
            RandomBoundedInt::new(202794285, 1, SupplierGenerator::SCALE_BASE);
        let mut bbb_junk_random = RowRandomInt::new(263032577, 1);
        let mut bbb_offset_random = RowRandomInt::new(715851524, 1);
        let mut bbb_type_random = RandomBoundedInt::new(753643799, 0, 100);

        // Advance all generators to the starting position
        address_random.advance_rows(start_index);
        nation_key_random.advance_rows(start_index);
        phone_random.advance_rows(start_index);
        account_balance_random.advance_rows(start_index);
        comment_random.advance_rows(start_index);
        bbb_comment_random.advance_rows(start_index);
        bbb_junk_random.advance_rows(start_index);
        bbb_offset_random.advance_rows(start_index);
        bbb_type_random.advance_rows(start_index);

        SupplierGeneratorIterator {
            address_random,
            nation_key_random,
            phone_random,
            account_balance_random,
            comment_random,
            bbb_comment_random,
            bbb_junk_random,
            bbb_offset_random,
            bbb_type_random,
            start_index,
            row_count,
            index: 0,
        }
    }

    /// Creates a supplier with the given key
    fn make_supplier(&mut self, supplier_key: i64) -> Supplier {
        let mut comment = self.comment_random.next_value();

        // Add supplier complaints or commendation to the comment
        let bbb_comment_random_value = self.bbb_comment_random.next_value();
        if bbb_comment_random_value <= SupplierGenerator::BBB_COMMENTS_PER_SCALE_BASE {
            let mut buffer = comment.clone();

            // select random place for BBB comment
            let noise = self.bbb_junk_random.next_int(
                0,
                (comment.len() - SupplierGenerator::BBB_COMMENT_LENGTH) as i32,
            ) as usize;
            let offset = self.bbb_offset_random.next_int(
                0,
                (comment.len() - (SupplierGenerator::BBB_COMMENT_LENGTH + noise)) as i32,
            ) as usize;

            // select complaint or recommendation
            let type_text =
                if self.bbb_type_random.next_value() < SupplierGenerator::BBB_COMPLAINT_PERCENT {
                    SupplierGenerator::BBB_COMPLAINT_TEXT
                } else {
                    SupplierGenerator::BBB_RECOMMEND_TEXT
                };

            // Create a mutable string that we can modify in chunks
            let mut modified_comment = String::with_capacity(comment.len());
            modified_comment.push_str(&comment[..offset]);
            modified_comment.push_str(SupplierGenerator::BBB_BASE_TEXT);
            modified_comment.push_str(
                &comment[offset + SupplierGenerator::BBB_BASE_TEXT.len()
                    ..offset + SupplierGenerator::BBB_BASE_TEXT.len() + noise],
            );
            modified_comment.push_str(type_text);
            modified_comment.push_str(
                &comment
                    [offset + SupplierGenerator::BBB_BASE_TEXT.len() + noise + type_text.len()..],
            );

            comment = modified_comment;
        }

        let nation_key = self.nation_key_random.next_value() as i64;

        Supplier {
            s_suppkey: supplier_key,
            s_name: format!("Supplier#{:09}", supplier_key),
            s_address: self.address_random.next_value(),
            s_nationkey: nation_key,
            s_phone: self.phone_random.next_value(nation_key),
            s_acctbal: self.account_balance_random.next_value() as f64 / 100.0,
            s_comment: comment,
        }
    }
}

impl Iterator for SupplierGeneratorIterator {
    type Item = Supplier;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.row_count {
            return None;
        }

        let supplier = self.make_supplier(self.start_index + self.index + 1);

        self.address_random.row_finished();
        self.nation_key_random.row_finished();
        self.phone_random.row_finished();
        self.account_balance_random.row_finished();
        self.comment_random.row_finished();
        self.bbb_comment_random.row_finished();
        self.bbb_junk_random.row_finished();
        self.bbb_offset_random.row_finished();
        self.bbb_type_random.row_finished();

        self.index += 1;

        Some(supplier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nation_generator() {
        let generator = NationGenerator::new();
        let nations: Vec<_> = generator.iter().collect();

        // TPC-H typically has 25 nations
        assert_eq!(nations.len(), 25);

        for nation in nations.iter().take(5) {
            println!("{nation}");
        }
    }

    #[test]
    fn test_region_generator() {
        let generator = RegionGenerator::new();
        let regions: Vec<_> = generator.iter().collect();

        // TPC-H typically has 5 regions
        assert_eq!(regions.len(), 5);
    }

    #[test]
    fn test_part_generation() {
        // Create a generator with a small scale factor
        let generator = PartGenerator::new(0.01, 1, 1);
        let parts: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 200,000 = 2,000 parts
        assert_eq!(parts.len(), 2000);

        // CSV Header like.
        println!("p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment");
        for part in parts.iter().take(5) {
            println!("{part}");
        }
    }

    #[test]
    fn test_calculate_part_price() {
        // Test with a few part keys
        assert_eq!(PartGeneratorIterator::calculate_part_price(1), 90100);
        assert_eq!(PartGeneratorIterator::calculate_part_price(10), 91001);
        assert_eq!(PartGeneratorIterator::calculate_part_price(100), 100010);
        assert_eq!(PartGeneratorIterator::calculate_part_price(1000), 90100);
    }

    #[test]
    fn test_supplier_generation() {
        // Create a generator with a small scale factor
        let generator = SupplierGenerator::new(0.01, 1, 1);
        let suppliers: Vec<_> = generator.iter().collect();

        // Should have 0.01 * 10,000 = 100 suppliers
        assert_eq!(suppliers.len(), 100);

        // Check first supplier
        let first = &suppliers[0];
        assert_eq!(first.s_suppkey, 1);
        assert_eq!(first.s_name, "Supplier#000000001");
        assert!(!first.s_address.is_empty());

        // Print the first 5 rows.
        for supplier in suppliers.iter().take(5) {
            println!("{supplier}");
        }
    }
}
