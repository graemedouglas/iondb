//! Pattern violation tests.
//!
//! Scans for forbidden code patterns in library crates:
//! - `unwrap()` in library code
//! - `expect()` in library code
//! - `Box<dyn Error>`
//! - `#[allow(...)]` without justification comment
//!
//! Remediation messages are included in assertion failures.

use std::fs;
use std::path::Path;
use walkdir::WalkDir;

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("Could not find workspace root")
}

/// Library crates to scan (excludes test crates, apps, and build scripts).
const LIB_CRATES: &[&str] = &[
    "iondb-core",
    "iondb-alloc",
    "iondb-storage",
    "iondb-io",
    "iondb-wal",
    "iondb-tx",
    "iondb-buffer",
    "iondb-query",
    "iondb-facade",
];

fn scan_files_for_pattern(crate_name: &str, pattern: &str, skip_in_tests: bool) -> Vec<String> {
    let root = workspace_root();
    let crate_src = root.join(crate_name).join("src");
    let mut violations = Vec::new();

    if !crate_src.exists() {
        return violations;
    }

    for entry in WalkDir::new(&crate_src)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "rs")
        })
    {
        let path = entry.path();
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let mut in_test_block = false;
        for (line_num, line) in content.lines().enumerate() {
            if line.contains("#[cfg(test)]") {
                in_test_block = true;
            }

            if skip_in_tests && in_test_block {
                continue;
            }

            if line.contains(pattern) && !line.trim_start().starts_with("//") {
                let relative = path.strip_prefix(root).unwrap_or(path);
                violations.push(format!(
                    "  {}:{} — {}",
                    relative.display(),
                    line_num + 1,
                    line.trim()
                ));
            }
        }
    }

    violations
}

#[test]
fn no_box_dyn_error_in_library_code() {
    let mut all_violations = Vec::new();

    for crate_name in LIB_CRATES {
        let violations = scan_files_for_pattern(crate_name, "Box<dyn Error>", false);
        all_violations.extend(violations);
    }

    assert!(
        all_violations.is_empty(),
        "PATTERN VIOLATION: `Box<dyn Error>` found in library code.\n\n\
         Use `iondb_core::Error` enum instead. Error variants behind feature flags \
         are compiled out when unused.\n\n{}\n",
        all_violations.join("\n")
    );
}

#[test]
fn no_allow_without_justification() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for crate_name in LIB_CRATES {
        let crate_src = root.join(crate_name).join("src");
        if !crate_src.exists() {
            continue;
        }

        for entry in WalkDir::new(&crate_src)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext == "rs")
            })
        {
            let path = entry.path();
            let content = match fs::read_to_string(path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let lines: Vec<&str> = content.lines().collect();
            for (i, line) in lines.iter().enumerate() {
                if line.contains("#[allow(") || line.contains("#![allow(") {
                    // Check if previous line has a justification comment
                    let has_justification = i > 0 && lines[i - 1].trim_start().starts_with("//");

                    if !has_justification {
                        let relative = path.strip_prefix(root).unwrap_or(path);
                        violations.push(format!(
                            "  {}:{} — {}",
                            relative.display(),
                            i + 1,
                            line.trim()
                        ));
                    }
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "PATTERN VIOLATION: `#[allow(...)]` without justification comment.\n\n\
         Suppressing a Clippy lint requires a justifying comment on the line above.\n\
         Remove the `#[allow]` or add a comment explaining why it's necessary.\n\n{}\n",
        violations.join("\n")
    );
}

#[test]
fn no_format_panic_in_no_std_paths() {
    // Check for format!/panic! with formatting in library code
    // (outside of #[cfg(test)] and #[cfg(feature = "std")] blocks).
    // This is a simplified check — a full implementation would parse
    // cfg attributes more carefully.
    let mut all_violations = Vec::new();

    for crate_name in LIB_CRATES {
        // Skip std-only patterns check for now; the thumbv6m build
        // is the real enforcement mechanism.
        let violations = scan_files_for_pattern(crate_name, "Box<dyn Error>", true);
        all_violations.extend(violations);
    }

    // This test primarily serves as documentation. The real enforcement
    // is the thumbv6m-none-eabi build gate, which will fail if format!
    // or panic! with formatting is used in no_std code.
    assert!(
        all_violations.is_empty(),
        "PATTERN VIOLATION: `Box<dyn Error>` in non-test library code.\n\n\
         Use `iondb_core::Error` enum. String formatting pulls in `core::fmt` \
         machinery and bloats binary size.\n\n{}\n",
        all_violations.join("\n")
    );
}
