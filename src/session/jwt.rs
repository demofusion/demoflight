use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::error::{DemoflightError, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionClaims {
    pub sub: String,
    pub iss: String,
    pub iat: i64,
    pub exp: i64,
    pub source_type: String,
    pub source_hash: String,
}

pub struct JwtHandler {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
}

impl JwtHandler {
    pub fn new(secret: &str) -> Self {
        let mut validation = Validation::default();
        validation.set_issuer(&["demoflight"]);
        validation.validate_exp = true;

        Self {
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
            validation,
        }
    }

    pub fn encode(&self, claims: SessionClaims) -> Result<String> {
        encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|e| DemoflightError::JwtValidation(e.to_string()))
    }

    pub fn decode(&self, token: &str) -> Result<SessionClaims> {
        let token_data = decode::<SessionClaims>(token, &self.decoding_key, &self.validation)
            .map_err(|e| DemoflightError::JwtValidation(e.to_string()))?;

        Ok(token_data.claims)
    }
}

pub fn sha256_prefix(input: &str, len: usize) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(input.as_bytes());
    hex::encode(&hash[..len.min(32)])
}

pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
