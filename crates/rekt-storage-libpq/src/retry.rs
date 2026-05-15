//! Exponential-backoff retry wrapper for transient PG errors.
//!
//! Applied at the trampoline level in [`crate::PgBackend`]: every
//! `Backend` method on `PgBackend` calls `self.with_retry("op_name",
//! || { … })` so a transient PG failure (08*/40001/40P01/57P0[1-5])
//! or a `BackendError::PoolExhausted` / `ConnectFailed` triggers up
//! to `max_attempts` retries with exponential backoff + jitter.
//!
//! Non-transient errors (`TableNotFound`, `ConditionalCheckFailed`,
//! every validation-class variant) bypass retry and propagate
//! immediately.
//!
//! The closure is `FnMut` so callers can mutate state across attempts
//! if needed; PgBackend's trampoline methods use plain async closures
//! that re-acquire a fresh client per attempt.

use rekt_storage::BackendError;
use std::future::Future;
use std::time::Duration;

/// Retry policy parameters. Mirrors `rekt_config::PgRetryConfig` but
/// owns its own copy so the storage layer doesn't depend on the
/// config loader. The binary populates this from
/// `config.pg.retry`.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub jitter_pct: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(1_000),
            jitter_pct: 25,
        }
    }
}

impl RetryPolicy {
    /// Disabled retry policy — `max_attempts = 1` runs the operation
    /// exactly once with no backoff. Used in tests that want
    /// deterministic single-shot semantics.
    pub fn disabled() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            jitter_pct: 0,
        }
    }
}

/// Decide whether an error is worth retrying.
///
/// Retryable:
/// - `PoolExhausted`: load may have dropped by the next attempt.
/// - `ConnectFailed`: TCP reset / DNS hiccup / timeout subclasses
///   resolve on a fresh attempt; auth failures don't, but we can't
///   discriminate cheaply.
/// - `PgError` with SQLSTATE in the transient set (see below).
///
/// Not retryable: every validation-class variant, table-not-found
/// (operator config issue), and `ConditionalCheckFailed` (semantic
/// outcome, not a transient failure — retrying could mask a real
/// race).
pub(crate) fn is_retryable(err: &BackendError) -> bool {
    match err {
        BackendError::PoolExhausted { .. } => true,
        BackendError::ConnectFailed { .. } => true,
        BackendError::PgError { sqlstate, .. } => is_transient_sqlstate(sqlstate),
        BackendError::ConditionalCheckFailed
        | BackendError::TableNotFound { .. }
        | BackendError::KeyTypeMismatch { .. }
        | BackendError::MissingSortKey { .. }
        | BackendError::UnexpectedSortKey { .. }
        | BackendError::Other(_) => false,
    }
}

/// Transient SQLSTATE classifier. Conservative — only the classes
/// that DDB itself would retry under throttling are included.
///
/// - `08*` — connection_exception family (lost connection)
/// - `40001` — serialization_failure
/// - `40P01` — deadlock_detected
/// - `57P01..57P05` — admin_shutdown / crash_shutdown /
///   cannot_connect_now / database_dropped
/// - `53*` — insufficient_resources family (e.g. `53300`
///   too_many_connections matches what PoolExhausted would catch
///   if it happened server-side rather than client-side)
pub(crate) fn is_transient_sqlstate(sqlstate: &str) -> bool {
    if sqlstate.len() < 5 {
        return false;
    }
    if sqlstate.starts_with("08") {
        return true;
    }
    if sqlstate.starts_with("53") {
        return true;
    }
    if sqlstate == "40001" || sqlstate == "40P01" {
        return true;
    }
    matches!(
        sqlstate,
        "57P01" | "57P02" | "57P03" | "57P04" | "57P05"
    )
}

/// Compute the backoff for `attempt` (0-indexed) given the policy.
/// Exponential doubling capped at `max_backoff`, then jittered by
/// `±jitter_pct%`. Jitter is sourced from `nanos_seed` (caller-
/// supplied for determinism in tests; otherwise pass nanos from a
/// clock).
pub(crate) fn backoff_for_attempt(
    policy: &RetryPolicy,
    attempt: u32,
    nanos_seed: u64,
) -> Duration {
    let base_ms = policy.initial_backoff.as_millis() as u64;
    let max_ms = policy.max_backoff.as_millis() as u64;
    // Exponential: base * 2^attempt, capped at max.
    let exp_ms = base_ms.saturating_mul(1u64 << attempt.min(20));
    let capped = exp_ms.min(max_ms);
    if policy.jitter_pct == 0 || capped == 0 {
        return Duration::from_millis(capped);
    }
    // Jitter: pick a value in [capped * (1 - p/100), capped * (1 + p/100)].
    let p = policy.jitter_pct as u64;
    let span = capped.saturating_mul(2 * p) / 100; // total jitter window
    let lo = capped.saturating_sub(capped.saturating_mul(p) / 100);
    let offset = nanos_seed % span.max(1);
    Duration::from_millis(lo.saturating_add(offset).min(capped + capped * p / 100))
}

/// Async retry wrapper. Calls `f` up to `policy.max_attempts` times,
/// sleeping with exponential backoff + jitter between attempts.
/// Logs at `warn!` per retry and at `error!` when the budget is
/// exhausted so operators see the timeline.
pub(crate) async fn with_retry<F, Fut, T>(
    policy: &RetryPolicy,
    op: &'static str,
    table: &str,
    mut f: F,
) -> Result<T, BackendError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, BackendError>>,
{
    let mut attempt: u32 = 0;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let last = attempt + 1 >= policy.max_attempts;
                if !is_retryable(&e) {
                    return Err(e);
                }
                if last {
                    tracing::error!(
                        op,
                        table,
                        attempts = attempt + 1,
                        error = %e,
                        "retry budget exhausted"
                    );
                    return Err(e);
                }
                let nanos_seed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_nanos() as u64)
                    .unwrap_or(0);
                let sleep_for = backoff_for_attempt(policy, attempt, nanos_seed);
                tracing::warn!(
                    op,
                    table,
                    attempt = attempt + 1,
                    sleep_ms = sleep_for.as_millis() as u64,
                    error = %e,
                    "retryable PG error; backing off"
                );
                tokio::time::sleep(sleep_for).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg_err(state: &str) -> BackendError {
        BackendError::PgError {
            sqlstate: state.into(),
            message: format!("synthetic {state}"),
        }
    }

    #[test]
    fn sqlstate_classifier_recognizes_transient_classes() {
        assert!(is_transient_sqlstate("08000")); // connection_exception
        assert!(is_transient_sqlstate("08006")); // connection_failure
        assert!(is_transient_sqlstate("40001")); // serialization_failure
        assert!(is_transient_sqlstate("40P01")); // deadlock_detected
        assert!(is_transient_sqlstate("57P01")); // admin_shutdown
        assert!(is_transient_sqlstate("57P03")); // cannot_connect_now
        assert!(is_transient_sqlstate("53300")); // too_many_connections

        // Non-transient — code path bugs.
        assert!(!is_transient_sqlstate("23505")); // unique_violation
        assert!(!is_transient_sqlstate("42P01")); // undefined_table
        assert!(!is_transient_sqlstate("22P02")); // invalid_text_representation
        assert!(!is_transient_sqlstate(""));
        assert!(!is_transient_sqlstate("1234")); // too short
    }

    #[test]
    fn is_retryable_targets_the_right_variants() {
        // Retryable
        assert!(is_retryable(&BackendError::PoolExhausted { waited_ms: 100 }));
        assert!(is_retryable(&BackendError::ConnectFailed {
            reason: "x".into()
        }));
        assert!(is_retryable(&pg_err("08006")));
        // Not retryable
        assert!(!is_retryable(&pg_err("23505")));
        assert!(!is_retryable(&BackendError::ConditionalCheckFailed));
        assert!(!is_retryable(&BackendError::TableNotFound {
            name: "t".into()
        }));
        assert!(!is_retryable(&BackendError::Other("misc".into())));
    }

    #[test]
    fn backoff_doubles_until_max() {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(800),
            jitter_pct: 0, // deterministic
        };
        // attempt 0 -> 100, attempt 1 -> 200, ..., attempt 3 -> 800 (cap)
        assert_eq!(backoff_for_attempt(&policy, 0, 0), Duration::from_millis(100));
        assert_eq!(backoff_for_attempt(&policy, 1, 0), Duration::from_millis(200));
        assert_eq!(backoff_for_attempt(&policy, 2, 0), Duration::from_millis(400));
        assert_eq!(backoff_for_attempt(&policy, 3, 0), Duration::from_millis(800));
        assert_eq!(backoff_for_attempt(&policy, 4, 0), Duration::from_millis(800));
        assert_eq!(backoff_for_attempt(&policy, 20, 0), Duration::from_millis(800));
    }

    #[test]
    fn jitter_keeps_value_in_band() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            jitter_pct: 25,
        };
        // Try a range of seeds; every result must be in [75ms, 125ms]
        // for attempt 0 (base = 100ms, +/- 25%).
        for seed in [0u64, 7, 13, 1_000, 999_999_999] {
            let d = backoff_for_attempt(&policy, 0, seed);
            let ms = d.as_millis() as u64;
            assert!(
                (75..=125).contains(&ms),
                "seed={seed} produced {ms}ms outside [75,125]"
            );
        }
    }

    #[tokio::test]
    async fn with_retry_succeeds_on_second_attempt() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            jitter_pct: 0,
        };
        let mut calls = 0u32;
        let result: Result<u32, BackendError> = with_retry(&policy, "test", "t", || {
            calls += 1;
            let attempt = calls;
            async move {
                if attempt < 2 {
                    Err(pg_err("08000"))
                } else {
                    Ok(42u32)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn with_retry_propagates_after_exhausting_budget() {
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            jitter_pct: 0,
        };
        let mut calls = 0u32;
        let result: Result<u32, BackendError> = with_retry(&policy, "test", "t", || {
            calls += 1;
            async move { Err::<u32, _>(pg_err("08000")) }
        })
        .await;
        assert!(matches!(
            result,
            Err(BackendError::PgError { ref sqlstate, .. }) if sqlstate == "08000"
        ));
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn with_retry_does_not_retry_non_transient_errors() {
        let policy = RetryPolicy::default();
        let mut calls = 0u32;
        let _: Result<u32, BackendError> = with_retry(&policy, "test", "t", || {
            calls += 1;
            async move { Err::<u32, _>(BackendError::ConditionalCheckFailed) }
        })
        .await;
        assert_eq!(calls, 1, "non-retryable error must fire once and propagate");
    }

    #[tokio::test]
    async fn with_retry_disabled_policy_runs_once() {
        let policy = RetryPolicy::disabled();
        let mut calls = 0u32;
        let _: Result<u32, BackendError> = with_retry(&policy, "test", "t", || {
            calls += 1;
            async move { Err::<u32, _>(pg_err("08000")) }
        })
        .await;
        assert_eq!(calls, 1);
    }
}
