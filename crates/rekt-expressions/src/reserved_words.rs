//! DynamoDB reserved-word list and matcher.
//!
//! DDB rejects bare attribute names in expressions that match any of
//! ~573 reserved words. Aliased names (via `ExpressionAttributeNames`,
//! i.e. `#foo`) bypass the check — that's the whole point of the
//! alias mechanism.
//!
//! The list is the canonical one from
//! <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html>.
//! Two preserved typos from the AWS list (`FLATTERN`, `INNTER`) are
//! kept verbatim so we match DDB's behavior exactly.
//!
//! Matching is case-insensitive ASCII (per the AWS docs).

/// Returns true if `name` is a DDB reserved word. Case-insensitive
/// ASCII match; non-ASCII names are never reserved (DDB allows them
/// as attribute names but they can't collide with the reserved set).
pub fn is_reserved(name: &str) -> bool {
    // Identifier characters are ASCII per the DDB expression grammar.
    // A name with any non-ASCII byte can't appear in the reserved list,
    // so fast-out.
    if !name.is_ascii() {
        return false;
    }
    // ASCII-lowercase comparison without allocating: walk the array
    // with `eq_ignore_ascii_case`. Binary search on a sorted-lowercase
    // slice is the cheap default; we sort once at module-init via the
    // hand-sorted const below.
    RESERVED_WORDS
        .binary_search_by(|w| {
            let a = w.as_bytes();
            let b = name.as_bytes();
            let n = a.len().min(b.len());
            for i in 0..n {
                let ai = a[i];
                let bi = b[i].to_ascii_lowercase();
                match ai.cmp(&bi) {
                    std::cmp::Ordering::Equal => {}
                    o => return o,
                }
            }
            a.len().cmp(&b.len())
        })
        .is_ok()
}

/// The reserved-word list, ASCII-lowercase, sorted ascending. Sort
/// invariant is checked by `reserved_list_is_sorted` in the test
/// module below.
const RESERVED_WORDS: &[&str] = &[
    "abort", "absolute", "action", "add", "after", "agent", "aggregate",
    "all", "allocate", "alter", "analyze", "and", "any", "archive",
    "are", "array", "as", "asc", "ascii", "asensitive", "assertion",
    "asymmetric", "at", "atomic", "attach", "attribute", "auth",
    "authorization", "authorize", "auto", "avg", "back", "backup",
    "base", "batch", "before", "begin", "between", "bigint", "binary",
    "bit", "blob", "block", "boolean", "both", "breadth", "bucket",
    "bulk", "by", "byte", "call", "called", "calling", "capacity",
    "cascade", "cascaded", "case", "cast", "catalog", "char",
    "character", "check", "class", "clob", "close", "cluster",
    "clustered", "clustering", "clusters", "coalesce", "collate",
    "collation", "collection", "column", "columns", "combine",
    "comment", "commit", "compact", "compile", "compress", "condition",
    "conflict", "connect", "connection", "consistency", "consistent",
    "constraint", "constraints", "constructor", "consumed", "continue",
    "convert", "copy", "corresponding", "count", "counter", "create",
    "cross", "cube", "current", "cursor", "cycle", "data", "database",
    "date", "datetime", "day", "deallocate", "dec", "decimal",
    "declare", "default", "deferrable", "deferred", "define", "defined",
    "definition", "delete", "delimited", "depth", "deref", "desc",
    "describe", "descriptor", "detach", "deterministic", "diagnostics",
    "directories", "disable", "disconnect", "distinct", "distribute",
    "do", "domain", "double", "drop", "dump", "duration", "dynamic",
    "each", "element", "else", "elseif", "empty", "enable", "end",
    "equal", "equals", "error", "escape", "escaped", "eval", "evaluate",
    "exceeded", "except", "exception", "exceptions", "exclusive",
    "exec", "execute", "exists", "exit", "explain", "explode", "export",
    "expression", "extended", "external", "extract", "fail", "false",
    "family", "fetch", "fields", "file", "filter", "filtering", "final",
    "finish", "first", "fixed", "flattern", "float", "for", "force",
    "foreign", "format", "forward", "found", "free", "from", "full",
    "function", "functions", "general", "generate", "get", "glob",
    "global", "go", "goto", "grant", "greater", "group", "grouping",
    "handler", "hash", "have", "having", "heap", "hidden", "hold",
    "hour", "identified", "identity", "if", "ignore", "immediate",
    "import", "in", "including", "inclusive", "increment", "incremental",
    "index", "indexed", "indexes", "indicator", "infinite", "initially",
    "inline", "inner", "innter", "inout", "input", "insensitive",
    "insert", "instead", "int", "integer", "intersect", "interval",
    "into", "invalidate", "is", "isolation", "item", "items", "iterate",
    "join", "key", "keys", "lag", "language", "large", "last",
    "lateral", "lead", "leading", "leave", "left", "length", "less",
    "level", "like", "limit", "limited", "lines", "list", "load",
    "local", "localtime", "localtimestamp", "location", "locator",
    "lock", "locks", "log", "loged", "long", "loop", "lower", "map",
    "match", "materialized", "max", "maxlen", "member", "merge",
    "method", "metrics", "min", "minus", "minute", "missing", "mod",
    "mode", "modifies", "modify", "module", "month", "multi",
    "multiset", "name", "names", "national", "natural", "nchar",
    "nclob", "new", "next", "no", "none", "not", "null", "nullif",
    "number", "numeric", "object", "of", "offline", "offset", "old",
    "on", "online", "only", "opaque", "open", "operator", "option",
    "or", "order", "ordinality", "other", "others", "out", "outer",
    "output", "over", "overlaps", "override", "owner", "pad",
    "parallel", "parameter", "parameters", "partial", "partition",
    "partitioned", "partitions", "path", "percent", "percentile",
    "permission", "permissions", "pipe", "pipelined", "plan", "pool",
    "position", "precision", "prepare", "preserve", "primary", "prior",
    "private", "privileges", "procedure", "processed", "project",
    "projection", "property", "provisioning", "public", "put", "query",
    "quit", "quorum", "raise", "random", "range", "rank", "raw",
    "read", "reads", "real", "rebuild", "record", "recursive", "reduce",
    "ref", "reference", "references", "referencing", "regexp", "region",
    "reindex", "relative", "release", "remainder", "rename", "repeat",
    "replace", "request", "reset", "resignal", "resource", "response",
    "restore", "restrict", "result", "return", "returning", "returns",
    "reverse", "revoke", "right", "role", "roles", "rollback", "rollup",
    "routine", "row", "rows", "rule", "rules", "sample", "satisfies",
    "save", "savepoint", "scan", "schema", "scope", "scroll", "search",
    "second", "section", "segment", "segments", "select", "self",
    "semi", "sensitive", "separate", "sequence", "serializable",
    "session", "set", "sets", "shard", "share", "shared", "short",
    "show", "signal", "similar", "size", "skewed", "smallint",
    "snapshot", "some", "source", "space", "spaces", "sparse",
    "specific", "specifictype", "split", "sql", "sqlcode", "sqlerror",
    "sqlexception", "sqlstate", "sqlwarning", "start", "state",
    "static", "status", "storage", "store", "stored", "stream",
    "string", "struct", "style", "sub", "submultiset", "subpartition",
    "substring", "subtype", "sum", "super", "symmetric", "synonym",
    "system", "table", "tablesample", "temp", "temporary", "terminated",
    "text", "than", "then", "throughput", "time", "timestamp",
    "timezone", "tinyint", "to", "token", "total", "touch", "trailing",
    "transaction", "transform", "translate", "translation", "treat",
    "trigger", "trim", "true", "truncate", "ttl", "tuple", "type",
    "under", "undo", "union", "unique", "unit", "unknown", "unlogged",
    "unnest", "unprocessed", "unsigned", "until", "update", "upper",
    "url", "usage", "use", "user", "users", "using", "uuid", "vacuum",
    "value", "valued", "values", "varchar", "variable", "variance",
    "varint", "varying", "view", "views", "virtual", "void", "wait",
    "when", "whenever", "where", "while", "window", "with", "within",
    "without", "work", "wrapped", "write", "year", "zone",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_list_is_sorted_and_lowercase() {
        for pair in RESERVED_WORDS.windows(2) {
            assert!(
                pair[0] < pair[1],
                "reserved-words list out of order: `{}` should come before `{}`",
                pair[0],
                pair[1]
            );
            assert!(
                pair[0].chars().all(|c| !c.is_ascii_uppercase()),
                "`{}` is not ASCII-lowercase",
                pair[0]
            );
        }
    }

    #[test]
    fn matches_canonical_count() {
        // The AWS doc page lists 573 words. If this count drifts, the
        // list was edited — verify against the source.
        assert_eq!(RESERVED_WORDS.len(), 573);
    }

    #[test]
    fn case_insensitive_match() {
        assert!(is_reserved("NAME"));
        assert!(is_reserved("name"));
        assert!(is_reserved("Name"));
        assert!(is_reserved("nAmE"));
    }

    #[test]
    fn known_reserved_words() {
        // These are the ones that have actually bitten this codebase
        // in diff tests against DDB-local.
        for w in ["name", "status", "counter", "source", "copy", "size"] {
            assert!(is_reserved(w), "expected `{w}` to be reserved");
        }
    }

    #[test]
    fn non_reserved_words() {
        // Safe attribute names — use these for main-path test logic.
        for w in [
            "id", "email", "score", "tags", "balance", "amount", "title",
            "label", "version", // not on AWS list despite being SQL-ish
        ] {
            assert!(!is_reserved(w), "expected `{w}` NOT to be reserved");
        }
    }

    #[test]
    fn boundary_cases() {
        // Empty string isn't reserved.
        assert!(!is_reserved(""));
        // Non-ASCII never matches (ASCII reserved list).
        assert!(!is_reserved("café"));
        // Random gibberish isn't reserved.
        assert!(!is_reserved("xyzqq"));
    }

    #[test]
    fn first_and_last_entries_match() {
        // Sanity-check the alphabetical boundaries.
        assert!(is_reserved("abort"));
        assert!(is_reserved("zone"));
    }
}
