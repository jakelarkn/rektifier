# Auth operator runbook

Workflows for the authentication surface shipped in PLAN-13.

## Master key

The AES-GCM credential encryption and the HMAC API-token pepper both
derive from a single operator-supplied 32-byte master key. Three
sources, exactly one configured when SigV4 or API tokens are enabled:

```toml
[auth.sigv4]
enabled = true
# Exactly one of:
master_key_env  = "REKTIFIER_AUTH_MASTER_KEY"
# master_key_file = "/etc/rektifier/master.key"
# master_key_kms  = "aws-kms://arn:..."   # A2a / deferred
```

The env / file value is 32 raw bytes, or 64 hex chars, or 44 chars
base64 (standard or URL-safe). Generate one with:

```sh
openssl rand -hex 32
```

**Rotation:** see "Master key rotation" below. The plaintext master
key lives in rektifier's process memory for the lifetime of the
process; never log it, never persist it anywhere outside the
configured source.

## AWS SigV4 credentials

### Add a credential

The operator owns AKID + secret_access_key generation. There is no
admin endpoint; the table is operator-managed via the rekt-auth
helpers or direct SQL with an AES-GCM encrypted secret.

```rust
use rekt_auth::{insert_credential_row, CredentialRow};
use rekt_auth::crypto::derive_subkey;
use rekt_auth::sigv4::verifier::SIGV4_AES_PURPOSE;

let aes_key = derive_subkey(&master_key, SIGV4_AES_PURPOSE);
insert_credential_row(&pool, &aes_key, &CredentialRow {
    access_key_id: "AKIAEXAMPLE".into(),
    secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
    principal: "aws:akid:AKIAEXAMPLE".into(),
    enabled: true,
}).await?;
```

### Rotate a credential

INSERT the new row, leave the old row enabled for at least
`SecretCache.ttl` (default 300s), then DELETE the old row.
Operators that flip directly from old to new risk a window where
in-flight requests using the old credential get rejected because
the cache hasn't expired yet.

### Disable without delete

`UPDATE _rektifier_aws_credentials SET enabled = false WHERE
access_key_id = '...'`. The next cache miss for that AKID will see
`enabled = false` and populate the negative cache. Operators that
need *immediate* effect should also invalidate the in-memory cache
— TODO: ship a rektifier admin endpoint for this (out of scope
here).

## API tokens

### Mint a token

```rust
use rekt_auth::{mint_api_token, TokenKind, insert_token_hash};
use rekt_auth::api_token::cache::{pepper_from_master, hmac_pepper};

let token = mint_api_token(TokenKind::Svc);
let pepper = pepper_from_master(&master_key);
let hash = hmac_pepper(&pepper, &token);
insert_token_hash(&pool, &hash, "myorg:service:foo", None).await?;

// Hand `token` to the caller; never store plaintext server-side.
println!("issued token: {token}");
```

The wire-format token is `rekt_svc_<28 chars base62>`. Tokens are
prefix-recognisable so operators can spot them in logs and
secret-scanners can register on the prefix.

### Rotate

Same as SigV4: mint new, leave old enabled for at least
`TokenCache.ttl`, then DELETE old.

### Revoke

`DELETE FROM _rektifier_api_tokens WHERE token_hash = ...`.
Propagation window = TokenCache.ttl on each rektifier instance.

## Master key rotation

The master key derives the AES-GCM DEK (for SigV4 secrets) and the
HMAC pepper (for API-token hashes). Rotating the master key
invalidates EVERY existing credential row and EVERY existing API
token. Plan accordingly:

1. Stand up a new master key alongside the old one.
2. For every SigV4 row in `_rektifier_aws_credentials`, decrypt with
   the old DEK and re-encrypt with the new DEK. Update the row in
   place.
3. For every API token in `_rektifier_api_tokens`, re-hash and
   replace. **The plaintext token is required for this step** — if
   it has been lost, the only path is to mint a fresh token and
   have the operator re-distribute it to the caller. Hash-only
   storage means there is no recoverable mapping from old pepper to
   new pepper.
4. Switch rektifier's configured master_key_env / file / kms to the
   new source.
5. Restart rektifier.

For step 3 the practical model is: when planning a master-key
rotation, send affected API-token holders new tokens *before* the
rotation, leave both enabled in PG (under the old and new peppers)
during a transition window, then DELETE the old-pepper rows after
the rotation completes.

## Role separation

Apply `docs/auth/role_separation.sql` once per database. The script
creates a `rektifier_app` role (rektifier connects as this) with
full DML on the auth + catalog tables, and a `rektifier_ro` role
with SELECT on user-data tables only — explicitly REVOKE'd from
the credential and API-token tables. Row-level security policies
back-stop the grants.

## Audit logging

When `[server] audit_log_enabled = true` (off by default), every
successful auth emits an info-level `auth.audit` line with target
`rektifier.audit` carrying:

- `principal`: the namespaced identity string
- `scheme`: `sigv4` / `jwt` / `api_token` / `permissive`
- `issuer`: JWT issuer URL when applicable, empty otherwise
- `claims`: JSON object filtered through the per-issuer
  safe-to-log allowlist (drops `email` / `name` / `phone_number` /
  custom non-allowlisted claims)

Direct the audit stream to a separate log handler via tracing's
target filter:

```
REKTIFIER_LOG="info,rektifier.audit=info"
```

Send `rektifier.audit` to whatever log sink the deployment requires
for retention / SIEM ingest.
