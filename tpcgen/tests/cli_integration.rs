use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use tempfile::tempdir;

/// Test the TPC-H command forms for the `tpcgen` binary.
#[test]
fn test_tpcgen_tpch_command_forms() {
    let forms: &[(&[&str], &[&str], &str)] = &[
        (&["tpch"], &[], "part.tbl"),
        (&["tpch", "tbl"], &[], "part.tbl"),
        (&["tpch", "csv"], &["--delimiter", "|"], "part.csv"),
        (
            &["tpch", "parquet"],
            &["--compression", "SNAPPY", "--row-group-bytes", "1000000"],
            "part.parquet",
        ),
    ];

    for (form, format_args, expected_file) in forms {
        let temp_dir = tempdir().expect("Failed to create temporary directory");

        cargo_bin_cmd!("tpcgen")
            .args(*form)
            .arg("--scale-factor")
            .arg("0.001")
            .arg("--tables")
            .arg("part")
            .arg("--output-dir")
            .arg(temp_dir.path())
            .arg("--no-progress")
            .args(*format_args)
            .assert()
            .success();

        let expected_file = temp_dir.path().join(expected_file);
        assert!(
            expected_file.exists(),
            "Expected file {:?} to exist with `tpcgen {}`",
            expected_file,
            form.join(" ")
        );
    }
}

/// Test the TPC-DS DAT command forms for the `tpcgen` binary.
#[test]
fn test_tpcgen_tpcds_dat_command_forms() {
    let forms: &[(&[&str], &str)] = &[
        (&["tpcds"], "reason.dat"),
        (&["tpcds", "dat"], "reason.dat"),
    ];

    for (form, expected_file) in forms {
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let output_dir = temp_dir.path().join("generated");

        cargo_bin_cmd!("tpcgen")
            .args(*form)
            .arg("--scale-factor")
            .arg("1")
            .arg("--tables")
            .arg("reason")
            .arg("--output-dir")
            .arg(&output_dir)
            .arg("--no-progress")
            .assert()
            .success();

        let expected_file = output_dir.join(expected_file);
        assert!(
            expected_file.exists(),
            "Expected file {:?} to exist with `tpcgen {}`",
            expected_file,
            form.join(" ")
        );

        let contents = fs::read_to_string(&expected_file).expect("Failed to read DAT file");
        assert!(
            contents.starts_with("1|AAAAAAAABAAAAAAA|Package was damaged|\n"),
            "Expected {:?} to contain deterministic pipe-delimited DAT output",
            expected_file
        );
        assert_eq!(
            contents.lines().count(),
            35,
            "Expected {:?} to contain the reason table at scale factor 1",
            expected_file
        );
    }
}

/// Test that TPC-DS DAT generation honors native TPC-DS options.
#[test]
fn test_tpcgen_tpcds_dat_options() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    cargo_bin_cmd!("tpcgen")
        .arg("tpcds")
        .arg("dat")
        .arg("--scale-factor")
        .arg("1")
        .arg("--tables")
        .arg("reason")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--suffix")
        .arg(".txt")
        .arg("--separator")
        .arg(",")
        .arg("--compat")
        .arg("trino")
        .arg("--no-progress")
        .assert()
        .success();

    let expected_file = temp_dir.path().join("reason.txt");
    let contents = fs::read_to_string(&expected_file).expect("Failed to read DAT file");
    assert!(
        contents.starts_with("1,AAAAAAAABAAAAAAA,Package was damaged,"),
        "Expected {:?} to honor custom suffix and separator",
        expected_file
    );
}

/// Test that TPC-DS progress bars are auto-suppressed when stderr is not a TTY.
#[test]
fn test_tpcgen_tpcds_progress_auto_disabled_on_non_tty() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    let output = cargo_bin_cmd!("tpcgen")
        .arg("tpcds")
        .arg("dat")
        .arg("--scale-factor")
        .arg("1")
        .arg("--tables")
        .arg("reason")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .assert()
        .success();

    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    for glyph in ["█", "▓", "░", "Progress:"] {
        assert!(
            !stderr.contains(glyph),
            "Expected progress to be auto-disabled on non-TTY stderr, but found {glyph:?} in: {stderr}"
        );
    }
}

/// Test that TPC-DS configures logging for the unified `tpcgen` binary.
#[test]
fn test_tpcgen_tpcds_verbose_configures_logging() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    let output = cargo_bin_cmd!("tpcgen")
        .arg("tpcds")
        .arg("dat")
        .arg("--scale-factor")
        .arg("1")
        .arg("--tables")
        .arg("reason")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--verbose")
        .arg("--no-progress")
        .assert()
        .success();

    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("Verbose output enabled"),
        "Expected verbose TPC-DS generation to initialize logging, got stderr: {stderr}"
    );
}

/// Test that non-DAT TPC-DS command forms still report that generation is unavailable.
#[test]
fn test_tpcgen_tpcds_non_dat_command_forms_are_not_implemented() {
    let forms: &[(&[&str], &[&str], &str)] = &[
        (&["tpcds", "csv"], &["--delimiter", "|"], "reason.csv"),
        (
            &["tpcds", "parquet"],
            &["--compression", "SNAPPY", "--row-group-bytes", "1000000"],
            "reason.parquet",
        ),
    ];

    for (form, format_args, unexpected_file) in forms {
        let temp_dir = tempdir().expect("Failed to create temporary directory");

        let assert = cargo_bin_cmd!("tpcgen")
            .args(*form)
            .arg("--scale-factor")
            .arg("1")
            .arg("--tables")
            .arg("reason")
            .arg("--output-dir")
            .arg(temp_dir.path())
            .arg("--quiet")
            .arg("--no-progress")
            .args(*format_args)
            .assert()
            .failure();

        let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
        assert!(
            stderr.contains("TPC-DS data generation is not yet implemented"),
            "Expected `tpcgen {}` to report that TPC-DS generation is not implemented, got stderr: {}",
            form.join(" "),
            stderr
        );

        let unexpected_file = temp_dir.path().join(unexpected_file);
        assert!(
            !unexpected_file.exists(),
            "Expected `tpcgen {}` not to create {:?}",
            form.join(" "),
            unexpected_file
        );
    }
}
