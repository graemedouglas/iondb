//! File size limit tests.
//!
//! Validates that no source file exceeds 500 lines.
//!
//! Remediation: "This file exceeds 500 lines. Split it into smaller modules
//! with clear responsibilities."

use std::fs;
use std::path::Path;
use walkdir::WalkDir;

const MAX_LINES: usize = 500;

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("Could not find workspace root")
}

fn is_source_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext == "rs")
}

fn should_check(path: &Path) -> bool {
    let path_str = path.to_string_lossy();
    // Skip target dir, git dir, and generated files
    !path_str.contains("/target/")
        && !path_str.contains("/.git/")
        && !path_str.contains("/mutants.out")
}

#[test]
fn no_source_file_exceeds_500_lines() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for entry in WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| is_source_file(e.path()))
        .filter(|e| should_check(e.path()))
    {
        let path = entry.path();
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let line_count = content.lines().count();
        if line_count > MAX_LINES {
            let relative = path.strip_prefix(root).unwrap_or(path);
            violations.push(format!("  {} ({line_count} lines)", relative.display()));
        }
    }

    assert!(
        violations.is_empty(),
        "FILE SIZE VIOLATION: The following files exceed {MAX_LINES} lines.\n\n\
         Split them into smaller modules with clear responsibilities.\n\n{}\n",
        violations.join("\n")
    );
}
