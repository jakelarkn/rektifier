//! SigV4 canonical-request + string-to-sign + signing-key derivation.
//!
//! Reference: <https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html>
//!
//! Procedure (from AWS docs):
//!
//! Canonical request:
//!   `HTTPMethod\nCanonicalURI\nCanonicalQueryString\nCanonicalHeaders\n\nSignedHeaders\nHashedPayload`
//!
//! String to sign:
//!   `AWS4-HMAC-SHA256\nX-Amz-Date\nCredentialScope\nHex(SHA256(CanonicalRequest))`
//!
//! Signing key (HMAC chain):
//!   `kDate    = HMAC("AWS4" + secret, date)`,
//!   `kRegion  = HMAC(kDate, region)`,
//!   `kService = HMAC(kRegion, service)`,
//!   `kSigning = HMAC(kService, "aws4_request")`.
//!
//! Signature = `Hex(HMAC(kSigning, StringToSign))`.

use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// AWS-canonical URI encoding: encode everything except unreserved
/// characters `A-Z a-z 0-9 - _ . ~`. The path "/" is preserved.
const AWS_URI_ENCODE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Per-path-segment encoding (preserves '/').
fn encode_path(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let mut out = String::with_capacity(path.len());
    for (i, segment) in path.split('/').enumerate() {
        if i > 0 {
            out.push('/');
        }
        out.push_str(&utf8_percent_encode(segment, AWS_URI_ENCODE).to_string());
    }
    out
}

/// Per-query-component encoding (encodes '=', '&', and unreserved
/// characters per AWS docs).
fn encode_query_component(s: &str) -> String {
    utf8_percent_encode(s, AWS_URI_ENCODE).to_string()
}

pub fn hex_sha256(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hex::encode(hasher.finalize())
}

/// Canonical query string per AWS docs: sort by key (then by value),
/// URI-encode key and value, join with `&` and `=`.
pub fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .map(|kv| {
            let mut iter = kv.splitn(2, '=');
            let k = iter.next().unwrap_or("");
            let v = iter.next().unwrap_or("");
            // The inbound query is *already* URI-encoded by the
            // HTTP client. Decode then re-encode for canonical form
            // (handles encoding-variation between clients).
            let k_dec = percent_encoding::percent_decode_str(k)
                .decode_utf8_lossy()
                .into_owned();
            let v_dec = percent_encoding::percent_decode_str(v)
                .decode_utf8_lossy()
                .into_owned();
            (encode_query_component(&k_dec), encode_query_component(&v_dec))
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Canonical headers per AWS docs:
/// - lowercase header names
/// - sorted by name
/// - trim leading/trailing whitespace from values, collapse internal
///   sequential whitespace
/// - one header per line `name:value\n`
pub fn canonical_headers(headers: &http::HeaderMap, signed_header_names: &[&str]) -> String {
    let mut entries: Vec<(String, String)> = Vec::with_capacity(signed_header_names.len());
    for name in signed_header_names {
        let lc = name.to_ascii_lowercase();
        // Concatenate all values for this header name (AWS joins
        // multi-value with ",").
        let mut joined = String::new();
        for v in headers.get_all(http::HeaderName::from_bytes(lc.as_bytes()).unwrap()).iter() {
            if !joined.is_empty() {
                joined.push(',');
            }
            // Trim + collapse whitespace.
            joined.push_str(&trim_collapse(v.to_str().unwrap_or("")));
        }
        entries.push((lc, joined));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = String::new();
    for (k, v) in entries {
        out.push_str(&k);
        out.push(':');
        out.push_str(&v);
        out.push('\n');
    }
    out
}

fn trim_collapse(s: &str) -> String {
    let trimmed = s.trim();
    // Collapse internal runs of whitespace to a single space.
    let mut out = String::with_capacity(trimmed.len());
    let mut prev_ws = false;
    for c in trimmed.chars() {
        if c.is_ascii_whitespace() {
            if !prev_ws {
                out.push(' ');
                prev_ws = true;
            }
        } else {
            out.push(c);
            prev_ws = false;
        }
    }
    out
}

/// Build the canonical request string.
pub fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &http::HeaderMap,
    signed_headers_csv: &str,
    payload_hash_hex: &str,
) -> String {
    let signed_header_names: Vec<&str> = signed_headers_csv.split(';').collect();
    let canonical_uri = encode_path(path);
    let canonical_qs = canonical_query_string(query);
    let canonical_h = canonical_headers(headers, &signed_header_names);
    format!(
        "{method}\n{canonical_uri}\n{canonical_qs}\n{canonical_h}\n{signed_headers}\n{payload_hash}",
        method = method,
        canonical_uri = canonical_uri,
        canonical_qs = canonical_qs,
        canonical_h = canonical_h,
        signed_headers = signed_headers_csv,
        payload_hash = payload_hash_hex,
    )
}

pub fn string_to_sign(amz_date: &str, credential_scope: &str, canonical_req: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hash}",
        hash = hex_sha256(canonical_req.as_bytes())
    )
}

pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any length");
        mac.update(data);
        let out = mac.finalize().into_bytes();
        let mut buf = [0u8; 32];
        buf.copy_from_slice(&out);
        buf
    }
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

pub fn compute_signature(signing_key: &[u8; 32], string_to_sign: &str) -> String {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(signing_key).expect("HMAC accepts any length");
    mac.update(string_to_sign.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    // AWS reference test vector — "get-vanilla" from the SigV4
    // test suite (us-east-1, host=example.amazonaws.com, GET /,
    // 20150830T123600Z, secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
    // akid = "AKIDEXAMPLE"). Expected signature is published.
    // <https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html>
    const AWS_SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const AWS_DATE: &str = "20150830";
    const AWS_AMZ_DATE: &str = "20150830T123600Z";
    const AWS_REGION: &str = "us-east-1";
    const AWS_SERVICE: &str = "service";
    const AWS_EXPECTED_SIGNATURE: &str =
        "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31";

    fn aws_reference_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("host", "example.amazonaws.com".parse().unwrap());
        h.insert("x-amz-date", AWS_AMZ_DATE.parse().unwrap());
        h
    }

    #[test]
    fn aws_reference_canonical_request_get_vanilla() {
        let req = canonical_request(
            "GET",
            "/",
            "",
            &aws_reference_headers(),
            "host;x-amz-date",
            // empty payload SHA256
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        let expected = "GET\n\
            /\n\
            \n\
            host:example.amazonaws.com\n\
            x-amz-date:20150830T123600Z\n\
            \n\
            host;x-amz-date\n\
            e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(req, expected);
    }

    #[test]
    fn aws_reference_string_to_sign() {
        let req = canonical_request(
            "GET",
            "/",
            "",
            &aws_reference_headers(),
            "host;x-amz-date",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        let scope = format!("{AWS_DATE}/{AWS_REGION}/{AWS_SERVICE}/aws4_request");
        let sts = string_to_sign(AWS_AMZ_DATE, &scope, &req);
        let expected = "AWS4-HMAC-SHA256\n\
            20150830T123600Z\n\
            20150830/us-east-1/service/aws4_request\n\
            bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63";
        assert_eq!(sts, expected);
    }

    #[test]
    fn aws_reference_signature_matches() {
        let req = canonical_request(
            "GET",
            "/",
            "",
            &aws_reference_headers(),
            "host;x-amz-date",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        let scope = format!("{AWS_DATE}/{AWS_REGION}/{AWS_SERVICE}/aws4_request");
        let sts = string_to_sign(AWS_AMZ_DATE, &scope, &req);
        let signing_k = signing_key(AWS_SECRET, AWS_DATE, AWS_REGION, AWS_SERVICE);
        let sig = compute_signature(&signing_k, &sts);
        assert_eq!(sig, AWS_EXPECTED_SIGNATURE);
    }

    #[test]
    fn canonical_query_string_sorts_and_encodes() {
        // foo=Zoo&foo=aha
        let qs = canonical_query_string("foo=Zoo&foo=aha");
        assert_eq!(qs, "foo=Zoo&foo=aha");
    }

    #[test]
    fn canonical_headers_trims_and_collapses_whitespace() {
        let mut h = HeaderMap::new();
        h.insert("my-header", "  hello   world  ".parse().unwrap());
        h.insert("host", "x".parse().unwrap());
        let out = canonical_headers(&h, &["host", "my-header"]);
        assert_eq!(out, "host:x\nmy-header:hello world\n");
    }

    #[test]
    fn encode_path_preserves_slashes() {
        assert_eq!(encode_path("/foo/bar"), "/foo/bar");
        assert_eq!(encode_path("/with space/here"), "/with%20space/here");
        assert_eq!(encode_path(""), "/");
    }
}
