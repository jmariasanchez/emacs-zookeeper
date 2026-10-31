//! ZooKeeper path utilities.
//!
//! These follow the server's path rules (`PathUtils.validatePath`): absolute,
//! no trailing slash except root, no empty, `.` or `..` components, and no
//! NUL or other disallowed characters.

use crate::error::{Result, ZkError};

/// Validates an absolute znode path.
pub fn validate(path: &str) -> Result<()> {
    validate_inner(path, false)
}

/// Validates a path that will be used as a sequential-node prefix (may end in `/`).
pub fn validate_sequential_prefix(path: &str) -> Result<()> {
    validate_inner(path, true)
}

fn validate_inner(path: &str, sequential: bool) -> Result<()> {
    let err = |reason: &str| ZkError::bad_arguments(format!("invalid path {path:?}: {reason}"));
    if path.is_empty() {
        return Err(err("empty"));
    }
    if !path.starts_with('/') {
        return Err(err("must be absolute"));
    }
    if path == "/" {
        return Ok(());
    }
    if path.ends_with('/') && !sequential {
        return Err(err("must not end with /"));
    }
    // A sequential prefix may end in '/', the server appends the sequence.
    let trimmed = if sequential { path.strip_suffix('/').unwrap_or(path) } else { path };
    if trimmed.is_empty() {
        return Ok(());
    }
    for (i, component) in trimmed[1..].split('/').enumerate() {
        if component.is_empty() {
            return Err(err(&format!("empty component at {i}")));
        }
        if component == "." || component == ".." {
            return Err(err("relative components are not allowed"));
        }
    }
    for c in path.chars() {
        let bad = c == '\0'
            || ('\u{0001}'..='\u{001F}').contains(&c)
            || ('\u{007F}'..='\u{009F}').contains(&c)
            || ('\u{e000}'..='\u{f8ff}').contains(&c)
            || ('\u{fff0}'..='\u{ffff}').contains(&c);
        if bad {
            return Err(err(&format!("disallowed character {c:?}")));
        }
    }
    Ok(())
}

/// Parent of `path`, or `None` for root.
pub fn parent(path: &str) -> Option<&str> {
    if path == "/" {
        return None;
    }
    match path.rfind('/') {
        Some(0) => Some("/"),
        Some(i) => Some(&path[..i]),
        None => None,
    }
}

/// Last component of `path` (empty for root).
pub fn basename(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

/// Joins a parent path and a child name.
pub fn join(parent: &str, child: &str) -> String {
    let child = child.trim_start_matches('/');
    if parent == "/" || parent.is_empty() {
        format!("/{child}")
    } else if child.is_empty() {
        parent.to_string()
    } else {
        format!("{}/{child}", parent.trim_end_matches('/'))
    }
}

/// Whether `path` equals `ancestor` or lies beneath it.
pub fn is_within(path: &str, ancestor: &str) -> bool {
    if ancestor == "/" {
        return true;
    }
    path == ancestor || (path.starts_with(ancestor) && path.as_bytes().get(ancestor.len()) == Some(&b'/'))
}

/// Path of `path` relative to `root` (`""` when equal). `None` if not within.
pub fn relative<'a>(path: &'a str, root: &str) -> Option<&'a str> {
    if !is_within(path, root) {
        return None;
    }
    if root == "/" {
        return Some(path.trim_start_matches('/'));
    }
    Some(path[root.len()..].trim_start_matches('/'))
}

/// Re-roots `path` from `from` to `to`.
pub fn rebase(path: &str, from: &str, to: &str) -> Option<String> {
    relative(path, from).map(|rel| if rel.is_empty() { to.to_string() } else { join(to, rel) })
}

/// Depth of a path (`/` = 0, `/a` = 1, ...).
pub fn depth(path: &str) -> usize {
    if path == "/" {
        0
    } else {
        path.matches('/').count()
    }
}

/// Matches `path` against a protection pattern.
///
/// Patterns are absolute paths; a trailing `/**` matches the node and every
/// descendant, a trailing `/*` matches direct children only, and `*` inside a
/// component matches any run of characters within that component.
pub fn matches_pattern(path: &str, pattern: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix("/**") {
        let prefix = if prefix.is_empty() { "/" } else { prefix };
        return glob_prefix_match(path, prefix, true);
    }
    glob_components(path, pattern)
}

fn glob_prefix_match(path: &str, prefix: &str, _recursive: bool) -> bool {
    if prefix == "/" {
        return true;
    }
    let pcomp: Vec<&str> = prefix[1..].split('/').collect();
    let comps: Vec<&str> = if path == "/" { vec![] } else { path[1..].split('/').collect() };
    if comps.len() < pcomp.len() {
        return false;
    }
    pcomp.iter().zip(comps.iter()).all(|(p, c)| glob_component(c, p))
}

fn glob_components(path: &str, pattern: &str) -> bool {
    if pattern == "/" {
        return path == "/";
    }
    if path == "/" {
        return false;
    }
    let pcomp: Vec<&str> = pattern.trim_start_matches('/').split('/').collect();
    let comps: Vec<&str> = path[1..].split('/').collect();
    pcomp.len() == comps.len() && pcomp.iter().zip(comps.iter()).all(|(p, c)| glob_component(c, p))
}

fn glob_component(text: &str, pattern: &str) -> bool {
    // Simple '*' wildcard matcher.
    let (t, p) = (text.as_bytes(), pattern.as_bytes());
    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == b'*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation() {
        assert!(validate("/").is_ok());
        assert!(validate("/a/b").is_ok());
        assert!(validate("a").is_err());
        assert!(validate("").is_err());
        assert!(validate("/a/").is_err());
        assert!(validate("/a//b").is_err());
        assert!(validate("/a/./b").is_err());
        assert!(validate("/a/../b").is_err());
        assert!(validate("/a\u{0}b").is_err());
        assert!(validate_sequential_prefix("/q/").is_ok());
        assert!(validate_sequential_prefix("/q/item-").is_ok());
    }

    #[test]
    fn navigation() {
        assert_eq!(parent("/"), None);
        assert_eq!(parent("/a"), Some("/"));
        assert_eq!(parent("/a/b"), Some("/a"));
        assert_eq!(basename("/a/b"), "b");
        assert_eq!(basename("/"), "");
        assert_eq!(join("/", "a"), "/a");
        assert_eq!(join("/a", "b"), "/a/b");
        assert_eq!(join("/a/", "/b"), "/a/b");
        assert_eq!(depth("/"), 0);
        assert_eq!(depth("/a/b"), 2);
    }

    #[test]
    fn containment() {
        assert!(is_within("/a/b", "/a"));
        assert!(is_within("/a", "/a"));
        assert!(!is_within("/ab", "/a"));
        assert!(is_within("/x", "/"));
        assert_eq!(relative("/a/b/c", "/a"), Some("b/c"));
        assert_eq!(relative("/a", "/a"), Some(""));
        assert_eq!(relative("/b", "/a"), None);
        assert_eq!(rebase("/a/b/c", "/a", "/z"), Some("/z/b/c".into()));
        assert_eq!(rebase("/a", "/a", "/z"), Some("/z".into()));
    }

    #[test]
    fn patterns() {
        assert!(matches_pattern("/prod", "/prod/**"));
        assert!(matches_pattern("/prod/a/b", "/prod/**"));
        assert!(!matches_pattern("/production", "/prod/**"));
        assert!(matches_pattern("/svc/x/config", "/svc/*/config"));
        assert!(!matches_pattern("/svc/x/y/config", "/svc/*/config"));
        assert!(matches_pattern("/kafka-1/brokers", "/kafka-*/brokers"));
        assert!(matches_pattern("/anything", "/**"));
        assert!(matches_pattern("/zookeeper/quota/x", "/zookeeper/**"));
        assert!(!matches_pattern("/a", "/b"));
        assert!(matches_pattern("/a", "/a"));
    }
}
