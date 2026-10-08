use std::{
    collections::HashMap,
    io::{self, BufReader, ErrorKind},
    path::Path,
};

use hostaddr::Domain;
use rcgen::KeyPair;
use rustls::{
    pki_types::{
        CertificateDer, PrivateKeyDer,
        pem::{self, SectionKind},
    },
    sign::CertifiedKey,
};
use tokio::fs;
use tracing::debug;

pub struct CertAndKey {
    pub cert: String,
    pub key: String,
}

impl CertAndKey {
    pub async fn read_from_disk(
        cache_dir: &Path,
        domain_name: &Domain<String>,
    ) -> Result<Option<CertAndKey>, io::Error> {
        let domain_path = cache_dir.join(&domain_name.to_string());
        let cert_path = domain_path.join("cert.pem");
        let key_path = domain_path.join("key.pem");

        match tokio::try_join!(fs::read_to_string(cert_path), fs::read_to_string(key_path)) {
            Ok((cert, key)) => Ok(Some(CertAndKey { cert, key })),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn read_all_from_disk(
        cache_dir: &Path,
    ) -> Result<HashMap<Domain<String>, CertAndKey>, io::Error> {
        let mut map = HashMap::new();

        let mut entries = fs::read_dir(cache_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }

            let dir_name = entry.file_name();
            let Some(dir_name) = dir_name.to_str() else {
                continue;
            };

            let Ok(domain_name) = dir_name.parse::<Domain<String>>().inspect_err(|e| {
                debug!(
                    "error parsing directory name \"{}\" into domain: {:?}",
                    dir_name, e
                )
            }) else {
                continue;
            };

            let Some(cert_and_key) = Self::read_from_disk(cache_dir, &domain_name).await? else {
                continue;
            };

            map.insert(domain_name, cert_and_key);
        }

        Ok(map)
    }

    pub async fn write_to_disk(
        &self,
        cache_dir: &Path,
        domain_name: &Domain<String>,
    ) -> Result<(), io::Error> {
        let domain_path = cache_dir.join(&domain_name.to_string());
        let cert_path = domain_path.join("cert.pem");
        let key_path = domain_path.join("key.pem");

        tokio::try_join!(
            fs::write(cert_path, &self.cert),
            fs::write(key_path, &self.key),
        )?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IntoCertifiedKeyError {
    #[error("parse cert error: {0}")]
    ParseCert(#[source] rustls::pki_types::pem::Error),

    #[error("unsepected section in cert: {0:?}")]
    ParseCertEnexpectedSection(SectionKind),

    #[error("parse key error: {0}")]
    ParseKey(#[source] rcgen::Error),

    #[error("parse private key der error: {0}")]
    ParsePrivateKeyDer(String),

    #[error("parse certified key error: {0}")]
    ParseCertifiedKey(#[source] rustls::Error),
}

impl TryInto<rustls::sign::CertifiedKey> for CertAndKey {
    type Error = IntoCertifiedKeyError;

    fn try_into(self) -> Result<CertifiedKey, Self::Error> {
        let mut cert_chain = Vec::new();
        let mut buf_reader = BufReader::new(self.cert.as_bytes());
        while let Some((kind, der)) =
            pem::from_buf(&mut buf_reader).map_err(IntoCertifiedKeyError::ParseCert)?
        {
            match kind {
                SectionKind::Certificate => {
                    cert_chain.push(CertificateDer::from(der));
                }
                kind => return Err(IntoCertifiedKeyError::ParseCertEnexpectedSection(kind)),
            };
        }

        let signing_key = KeyPair::from_pem(&self.key).map_err(IntoCertifiedKeyError::ParseKey)?;
        let private_key = PrivateKeyDer::try_from(signing_key.serialize_der())
            .map_err(|e| IntoCertifiedKeyError::ParsePrivateKeyDer(e.to_string()))?;

        Ok(CertifiedKey::from_der(
            cert_chain,
            private_key,
            &rustls::crypto::aws_lc_rs::default_provider(),
        )
        .map_err(IntoCertifiedKeyError::ParseCertifiedKey)?)
    }
}
