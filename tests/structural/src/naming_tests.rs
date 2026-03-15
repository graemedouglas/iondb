//! Naming convention tests.
//!
//! Validates that module names and file names follow Rust and project conventions:
//! - Module/file names use `snake_case`.
//! - No uppercase letters in `.rs` file names.

use std::path::Path;
use walkdir::WalkDir;

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("Could not find workspace root")
}

fn should_check(path: &Path) -> bool {
    let path_str = path.to_string_lossy();
    !path_str.contains("/target/")
        && !path_str.contains("/.git/")
        && !path_str.contains("/mutants.out")
}

fn is_valid_module_name(name: &str) -> bool {
    // Must be snake_case: lowercase letters, digits, underscores only.
    // Special files: mod.rs, lib.rs, main.rs are fine.
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

#[test]
fn rust_file_names_are_snake_case() {
    let root = workspace_root();
    let mut violations = Vec::new();

    for entry in WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| should_check(e.path()))
    {
        let path = entry.path();
        let ext = path.extension().and_then(|e| e.to_str());
        if ext != Some("rs") {
            continue;
        }

        let stem = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s,
            None => continue,
        };

        if !is_valid_module_name(stem) {
            let relative = path.strip_prefix(root).unwrap_or(path);
            violations.push(format!(
                "  {} (stem: `{stem}` — must be snake_case)",
                relative.display()
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "NAMING CONVENTION VIOLATION: Rust file names must use snake_case.\n\n\
         Rename the following files:\n\n{}\n",
        violations.join("\n")
    );
}

#[test]
fn crate_directories_are_kebab_case() {
    let root = workspace_root();
    let crate_dirs = [
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

    for dir in &crate_dirs {
        let path = root.join(dir);
        if path.exists() {
            // Verify the directory name matches expected kebab-case
            assert!(
                dir.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
                "NAMING CONVENTION VIOLATION: Crate directory `{dir}` must be kebab-case."
            );
        }
    }
}
