use rustls::{pki_types::PrivateKeyDer, sign::CertifiedKey};

pub trait SelfSignedCertifiedKeyExt {
    type Error;

    fn self_signed(subject_alt_names: impl Into<Vec<String>>) -> Result<CertifiedKey, Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum SelfSignedCertifiedKeyError {
    #[error("invalid subject alt name: {0}")]
    InvalidSubjectAltName(#[source] rcgen::Error),

    #[error("signing key generation failed: {0}")]
    SigningKeyGenerationFailed(#[source] rcgen::Error),

    #[error("certificate generation failed: {0}")]
    CertificateGenerationFailed(#[source] rcgen::Error),

    #[error("private key generation failed: {0}")]
    PrivateKeyGenerationFailed(String),

    #[error("certified key generation failed: {0}")]
    CertifiedKeyGenerationFailed(#[source] rustls::Error),
}

impl SelfSignedCertifiedKeyExt for CertifiedKey {
    type Error = SelfSignedCertifiedKeyError;

    fn self_signed(subject_alt_names: impl Into<Vec<String>>) -> Result<CertifiedKey, Self::Error> {
        let mut cert_params = rcgen::CertificateParams::new(subject_alt_names)
            .map_err(SelfSignedCertifiedKeyError::InvalidSubjectAltName)?;

        cert_params.not_before = time::OffsetDateTime::now_utc() - time::Duration::minutes(1);
        cert_params.not_after = cert_params.not_before + time::Duration::days(7);

        let signing_key = rcgen::KeyPair::generate()
            .map_err(SelfSignedCertifiedKeyError::SigningKeyGenerationFailed)?;
        let cert = cert_params
            .self_signed(&signing_key)
            .map_err(SelfSignedCertifiedKeyError::CertificateGenerationFailed)?;

        let certified_key = CertifiedKey::from_der(
            vec![cert.der().to_owned()],
            PrivateKeyDer::try_from(signing_key.serialize_der()).map_err(|e| {
                SelfSignedCertifiedKeyError::PrivateKeyGenerationFailed(e.to_owned())
            })?,
            &rustls::crypto::aws_lc_rs::default_provider(),
        )
        .map_err(SelfSignedCertifiedKeyError::CertifiedKeyGenerationFailed)?;

        Ok(certified_key)
    }
}
