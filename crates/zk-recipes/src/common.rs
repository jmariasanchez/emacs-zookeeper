//! Helpers shared by recipes: sequential node naming and ordering.

use zk_core::{path, ErrorKind, Result, ZkError};

/// Parses the 10 (or 19) digit sequence suffix ZooKeeper appends to sequential nodes.
pub fn sequence_of(name: &str) -> Option<i64> {
    let digits = name.len() - name.bytes().rev().take_while(|b| b.is_ascii_digit()).count();
    let suffix = &name[digits..];
    if suffix.len() >= 10 {
        suffix.parse().ok()
    } else {
        None
    }
}

/// Sorts child names by their sequence suffix, dropping names without one.
/// Names that share a prefix filter are kept when `filter` returns true.
pub fn sort_by_sequence<'a>(children: impl IntoIterator<Item = &'a String>, filter: impl Fn(&str) -> bool) -> Vec<(i64, String)> {
    let mut v: Vec<(i64, String)> = children
        .into_iter()
        .filter(|c| filter(c))
        .filter_map(|c| sequence_of(c).map(|s| (s, c.clone())))
        .collect();
    v.sort();
    v
}

/// Validates a recipe base path.
pub fn validate_base(base: &str) -> Result<()> {
    path::validate(base)?;
    if base == "/" {
        return Err(ZkError::bad_arguments("recipe base path must not be /"));
    }
    Ok(())
}

/// Error for an ownership that was lost while waiting.
pub fn lost(what: &str) -> ZkError {
    ZkError::new(ErrorKind::SessionExpired, format!("{what}: session lost while waiting"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequences() {
        assert_eq!(sequence_of("lock-0000000012"), Some(12));
        assert_eq!(sequence_of("_c_abc-lock-0000000003"), Some(3));
        assert_eq!(sequence_of("x-12"), None);
        let names = vec!["b-0000000002".to_string(), "a-0000000001".to_string(), "junk".to_string()];
        let s = sort_by_sequence(&names, |_| true);
        assert_eq!(s, vec![(1, "a-0000000001".into()), (2, "b-0000000002".into())]);
    }
}
