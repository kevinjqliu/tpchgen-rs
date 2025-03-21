//! Rust TPCH Data Generator
//!
//! This crate provides a Rust implementation of the TPC-H data generator.
//!
//! # Example: TBL output format
//! ```
//! # use tpchgen::generators::LineItemGenerator;
//! // Create Generator for the LINEITEM table at Scale Factor 1 (SF 1)
//! let scale_factor = 1.0;
//! let part = 1;
//! let num_parts = 1;
//! let generator = LineItemGenerator::new(scale_factor, part, num_parts);
//!
//! // Output the first 3 rows in classic TPCH TBL format
//! // (the generators are normal rust iterators and combine well with the Rust ecosystem)
//! let lines: Vec<_> = generator.iter()
//!    .take(3)
//!    .map(|line| line.to_string()) // use Display impl to get TBL format
//!    .collect::<Vec<_>>();
//!  assert_eq!(
//!   lines.join("\n"),"\
//!   1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|\n\
//!   1|67310|7311|2|36|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |\n\
//!   1|63700|3701|3|8|13309.60|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep|"
//!   );
//! ```
//!
//! Each generator produces a row struct (e.g. [`LineItem`]) that is designed
//! to be efficiently converted to the output format (e.g. TBL CSV). This crate
//! provides the following output formats:
//!
//! - TBL: The `Display` impl of the row structs produces the TPCH TBL format.
//!
//! [`LineItem`]: generators::LineItem

pub mod dates;
pub mod decimal;
pub mod distribution;
pub mod generators;
pub mod random;
pub mod text;
