//! Consistence and conformance test suite that runs against Trino's TPCH
//! Java implementation.
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSupplierGenerator, RegionGenerator, SupplierGenerator,
};

fn read_tbl_gz<P: AsRef<Path>>(path: P) -> Vec<String> {
    let file = File::open(path).expect("Failed to open file");
    let gz = GzDecoder::new(file);
    let reader = BufReader::new(gz);
    reader
        .lines()
        .collect::<Result<_, _>>()
        .expect("Failed to read lines")
}

fn test_generator<T, I>(iter: I, reference_path: &str, transform_fn: impl Fn(T) -> String)
where
    I: Iterator<Item = T>,
{
    let mut dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    dir.push(reference_path);

    // Read reference data.
    let reference_data = read_tbl_gz(dir);

    // Generate data using our own generators.
    let generated_data: Vec<String> = iter.map(transform_fn).collect();

    // Compare that we have the same number of records.
    assert_eq!(
        reference_data.len(),
        generated_data.len(),
        "Number of records doesn't match for {}. Reference: {}, Generated: {}",
        reference_path,
        reference_data.len(),
        generated_data.len()
    );

    for (i, (reference, generated)) in reference_data.iter().zip(generated_data.iter()).enumerate()
    {
        assert_eq!(
            reference, generated,
            "Record {} doesn't match for {}.\nReference: {}\nGenerated: {}",
            i, reference_path, reference, generated
        );
    }
}

#[test]
fn test_nation_sf_0_001() {
    let _sf = 0.001;
    let generator = NationGenerator::new();
    test_generator(generator.iter(), "data/sf-0.001/nation.tbl.gz", |nation| {
        format!(
            "{}|{}|{}|{}|",
            nation.n_nationkey, nation.n_name, nation.n_regionkey, nation.n_comment
        )
    });
}

#[test]
fn test_region_sf_0_001() {
    let _sf = 0.001;
    let generator = RegionGenerator::new();
    test_generator(generator.iter(), "data/sf-0.001/region.tbl.gz", |region| {
        format!(
            "{}|{}|{}|",
            region.r_regionkey, region.r_name, region.r_comment
        )
    });
}

#[test]
fn test_part_sf_0_001() {
    let sf = 0.001;
    let generator = PartGenerator::new(sf, 1, 1);
    test_generator(generator.iter(), "data/sf-0.001/part.tbl.gz", |part| {
        format!(
            "{}|{}|{}|{}|{}|{}|{}|{:.2}|{}|",
            part.p_partkey,
            part.p_name,
            part.p_mfgr,
            part.p_brand,
            part.p_type,
            part.p_size,
            part.p_container,
            part.p_retailprice,
            part.p_comment
        )
    });
}

#[test]
fn test_supplier_sf_0_001() {
    let sf = 0.001;
    let generator = SupplierGenerator::new(sf, 1, 1);
    test_generator(
        generator.iter(),
        "data/sf-0.001/supplier.tbl.gz",
        |supplier| {
            format!(
                "{}|{}|{}|{}|{}|{:.2}|{}|",
                supplier.s_suppkey,
                supplier.s_name,
                supplier.s_address,
                supplier.s_nationkey,
                supplier.s_phone,
                supplier.s_acctbal,
                supplier.s_comment
            )
        },
    );
}

#[test]
fn test_partsupp_sf_0_001() {
    let sf = 0.001;
    let generator = PartSupplierGenerator::new(sf, 1, 1);
    test_generator(generator.iter(), "data/sf-0.001/partsupp.tbl.gz", |ps| {
        format!(
            "{}|{}|{}|{:.2}|{}|",
            ps.ps_partkey, ps.ps_suppkey, ps.ps_availqty, ps.ps_supplycost, ps.ps_comment
        )
    });
}

#[test]
fn test_customer_sf_0_001() {
    let sf = 0.001;
    let generator = CustomerGenerator::new(sf, 1, 1);
    test_generator(
        generator.iter(),
        "data/sf-0.001/customer.tbl.gz",
        |customer| {
            format!(
                "{}|{}|{}|{}|{}|{:.2}|{}|{}|",
                customer.c_custkey,
                customer.c_name,
                customer.c_address,
                customer.c_nationkey,
                customer.c_phone,
                customer.c_acctbal,
                customer.c_mktsegment,
                customer.c_comment
            )
        },
    );
}

#[test]
fn test_orders_sf_0_001() {
    let sf = 0.001;
    let generator = OrderGenerator::new(sf, 1, 1);
    test_generator(generator.iter(), "data/sf-0.001/orders.tbl.gz", |order| {
        format!(
            "{}|{}|{}|{:.2}|{}|{}|{}|{}|{}|",
            order.o_orderkey,
            order.o_custkey,
            order.o_orderstatus,
            order.o_totalprice,
            order.o_orderdate,
            order.o_orderpriority,
            order.o_clerk,
            order.o_shippriority,
            order.o_comment
        )
    });
}

#[test]
fn test_lineitem_sf_0_001() {
    let sf = 0.001;
    let generator = LineItemGenerator::new(sf, 1, 1);
    test_generator(generator.iter(), "data/sf-0.001/lineitem.tbl.gz", |item| {
        format!(
            "{}|{}|{}|{}|{:.2}|{:.2}|{:.2}|{:.2}|{}|{}|{}|{}|{}|{}|{}|{}|",
            item.l_orderkey,
            item.l_partkey,
            item.l_suppkey,
            item.l_linenumber,
            item.l_quantity,
            item.l_extendedprice,
            item.l_discount,
            item.l_tax,
            item.l_returnflag,
            item.l_linestatus,
            item.l_shipdate,
            item.l_commitdate,
            item.l_receiptdate,
            item.l_shipinstruct,
            item.l_shipmode,
            item.l_comment
        )
    });
}

#[test]
fn test_nation_sf_0_01() {
    let _sf = 0.01;
    let generator = NationGenerator::new();
    test_generator(generator.iter(), "data/sf-0.01/nation.tbl.gz", |nation| {
        format!(
            "{}|{}|{}|{}|",
            nation.n_nationkey, nation.n_name, nation.n_regionkey, nation.n_comment
        )
    });
}

#[test]
fn test_region_sf_0_01() {
    let _sf = 0.01;
    let generator = RegionGenerator::new();
    test_generator(generator.iter(), "data/sf-0.01/region.tbl.gz", |region| {
        format!(
            "{}|{}|{}|",
            region.r_regionkey, region.r_name, region.r_comment
        )
    });
}

#[test]
fn test_part_sf_0_01() {
    let sf = 0.01;
    let generator = PartGenerator::new(sf, 1, 1);
    test_generator(generator.iter(), "data/sf-0.01/part.tbl.gz", |part| {
        format!(
            "{}|{}|{}|{}|{}|{}|{}|{:.2}|{}|",
            part.p_partkey,
            part.p_name,
            part.p_mfgr,
            part.p_brand,
            part.p_type,
            part.p_size,
            part.p_container,
            part.p_retailprice,
            part.p_comment
        )
    });
}

#[test]
fn test_supplier_sf_0_01() {
    let sf = 0.01;
    let generator = SupplierGenerator::new(sf, 1, 1);
    test_generator(
        generator.iter(),
        "data/sf-0.01/supplier.tbl.gz",
        |supplier| {
            format!(
                "{}|{}|{}|{}|{}|{:.2}|{}|",
                supplier.s_suppkey,
                supplier.s_name,
                supplier.s_address,
                supplier.s_nationkey,
                supplier.s_phone,
                supplier.s_acctbal,
                supplier.s_comment
            )
        },
    );
}

#[test]
fn test_partsupp_sf_0_01() {
    let sf = 0.01;
    let generator = PartSupplierGenerator::new(sf, 1, 1);
    test_generator(generator.iter(), "data/sf-0.01/partsupp.tbl.gz", |ps| {
        format!(
            "{}|{}|{}|{:.2}|{}|",
            ps.ps_partkey, ps.ps_suppkey, ps.ps_availqty, ps.ps_supplycost, ps.ps_comment
        )
    });
}

#[test]
fn test_customer_sf_0_01() {
    let sf = 0.01;
    let generator = CustomerGenerator::new(sf, 1, 1);
    test_generator(
        generator.iter(),
        "data/sf-0.01/customer.tbl.gz",
        |customer| {
            format!(
                "{}|{}|{}|{}|{}|{:.2}|{}|{}|",
                customer.c_custkey,
                customer.c_name,
                customer.c_address,
                customer.c_nationkey,
                customer.c_phone,
                customer.c_acctbal,
                customer.c_mktsegment,
                customer.c_comment
            )
        },
    );
}

#[test]
fn test_orders_sf_0_01() {
    let sf = 0.01;
    let generator = OrderGenerator::new(sf, 1, 1);
    test_generator(generator.iter(), "data/sf-0.01/orders.tbl.gz", |order| {
        format!(
            "{}|{}|{}|{:.2}|{}|{}|{}|{}|{}|",
            order.o_orderkey,
            order.o_custkey,
            order.o_orderstatus,
            order.o_totalprice,
            order.o_orderdate,
            order.o_orderpriority,
            order.o_clerk,
            order.o_shippriority,
            order.o_comment
        )
    });
}

#[test]
fn test_lineitem_sf_0_01() {
    let sf = 0.01;
    let generator = LineItemGenerator::new(sf, 1, 1);
    test_generator(generator.iter(), "data/sf-0.01/lineitem.tbl.gz", |item| {
        format!(
            "{}|{}|{}|{}|{}|{:.2}|{:.2}|{:.2}|{}|{}|{}|{}|{}|{}|{}|{}|",
            item.l_orderkey,
            item.l_partkey,
            item.l_suppkey,
            item.l_linenumber,
            item.l_quantity,
            item.l_extendedprice,
            item.l_discount,
            item.l_tax,
            item.l_returnflag,
            item.l_linestatus,
            item.l_shipdate,
            item.l_commitdate,
            item.l_receiptdate,
            item.l_shipinstruct,
            item.l_shipmode,
            item.l_comment
        )
    });
}
