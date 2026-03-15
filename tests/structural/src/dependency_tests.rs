//! Dependency direction tests.
//!
//! Validates that no implementation crate depends on another implementation
//! crate. Cross-crate integration belongs in `iondb-tx` or the facade `iondb`.
//!
//! Remediation: "This crate depends on another implementation crate.
//! Cross-crate integration belongs in `iondb-tx` or the facade `iondb` crate.
//! See `docs/requirements/initial/v0.md` §7.2."

use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// Implementation crates that must NOT depend on each other.
const IMPL_CRATES: &[&str] = &[
    "iondb-alloc",
    "iondb-storage",
    "iondb-io",
    "iondb-wal",
    "iondb-buffer",
    "iondb-query",
];

/// Crates that are allowed as dependencies for impl crates.
const ALLOWED_DEPS: &[&str] = &["iondb-core"];

fn workspace_root() -> &'static Path {
    // Walk up from the test binary to find the workspace root.
    // The structural tests crate is at tests/structural/ relative to root.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("Could not find workspace root. Expected tests/structural/ to be two levels below root.")
}

fn parse_workspace_deps(cargo_toml_content: &str) -> HashSet<String> {
    let parsed: toml::Value = cargo_toml_content
        .parse()
        .expect("Failed to parse Cargo.toml");

    let mut deps = HashSet::new();

    if let Some(dep_table) = parsed.get("dependencies").and_then(|d| d.as_table()) {
        for key in dep_table.keys() {
            deps.insert(key.clone());
        }
    }

    deps
}

#[test]
fn no_horizontal_dependencies_between_impl_crates() {
    let root = workspace_root();
    let impl_crate_names: HashSet<&str> = IMPL_CRATES.iter().copied().collect();
    let allowed: HashSet<&str> = ALLOWED_DEPS.iter().copied().collect();

    for crate_name in IMPL_CRATES {
        let cargo_path = root.join(crate_name).join("Cargo.toml");
        if !cargo_path.exists() {
            continue;
        }

        let content = fs::read_to_string(&cargo_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {e}", cargo_path.display()));

        let deps = parse_workspace_deps(&content);

        for dep in &deps {
            let dep_str = dep.as_str();
            if impl_crate_names.contains(dep_str) && !allowed.contains(dep_str) {
                panic!(
                    "DEPENDENCY VIOLATION: `{crate_name}` depends on `{dep}`.\n\n\
                     Cross-crate integration belongs in `iondb-tx` or the facade `iondb` crate.\n\
                     See `docs/requirements/initial/v0.md` §7.2.\n\n\
                     Implementation crates may only depend on `iondb-core`."
                );
            }
        }
    }
}

#[test]
fn iondb_core_depends_on_nothing() {
    let root = workspace_root();
    let cargo_path = root.join("iondb-core").join("Cargo.toml");
    if !cargo_path.exists() {
        return;
    }

    let content = fs::read_to_string(&cargo_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {e}", cargo_path.display()));

    let deps = parse_workspace_deps(&content);

    // iondb-core should have no workspace dependencies
    let workspace_deps: Vec<&String> = deps.iter().filter(|d| d.starts_with("iondb-")).collect();

    assert!(
        workspace_deps.is_empty(),
        "DEPENDENCY VIOLATION: `iondb-core` must depend on nothing (except core/alloc).\n\n\
         Found dependencies: {workspace_deps:?}\n\n\
         `iondb-core` is the leaf of the dependency tree. Move these dependencies \
         to the appropriate implementation crate."
    );
}

#[test]
fn impl_crates_depend_on_iondb_core() {
    let root = workspace_root();

    for crate_name in IMPL_CRATES {
        let cargo_path = root.join(crate_name).join("Cargo.toml");
        if !cargo_path.exists() {
            continue;
        }

        let content = fs::read_to_string(&cargo_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {e}", cargo_path.display()));

        let deps = parse_workspace_deps(&content);

        assert!(
            deps.contains("iondb-core"),
            "DEPENDENCY VIOLATION: `{crate_name}` must depend on `iondb-core`.\n\n\
             All implementation crates share traits and types via `iondb-core`."
        );
    }
}
