use std::sync::Arc;

use dashmap::DashMap;
use hostaddr::Domain;
use rustls::{server::ResolvesServerCert, sign::CertifiedKey};
use tracing::debug;

use crate::tls_util::SelfSignedCertifiedKeyExt;

#[derive(Debug, Clone)]
pub struct DynamicCertResolver {
    ca_signed_certs: DashMap<Domain<String>, Arc<CertifiedKey>>,
    self_signed_certs: DashMap<Domain<String>, Arc<CertifiedKey>>,
}

impl DynamicCertResolver {
    pub fn new() -> Self {
        Self {
            ca_signed_certs: DashMap::new(),
            self_signed_certs: DashMap::new(),
        }
    }

    pub fn insert(&self, domain_name: Domain<String>, certified_key: CertifiedKey) {
        self.ca_signed_certs
            .insert(domain_name, Arc::new(certified_key));
    }

    pub fn get(&self, domain_name: &Domain<String>) -> Option<Arc<CertifiedKey>> {
        if let Some(certified_key) = self.ca_signed_certs.get(domain_name) {
            return Some(certified_key.to_owned());
        };

        if let Some(certified_key) = self.self_signed_certs.get(domain_name) {
            return Some(certified_key.to_owned());
        };

        return None;
    }

    pub fn get_or_self_sign(&self, domain_name: Domain<String>) -> Option<Arc<CertifiedKey>> {
        if let Some(certified_key) = self.get(&domain_name) {
            return Some(certified_key.to_owned());
        };

        let self_signed_cert = match CertifiedKey::self_signed(vec![domain_name.to_string()]) {
            Ok(self_signed_cert) => Arc::new(self_signed_cert),
            Err(e) => {
                debug!(
                    "Unable to generate self signed cert for \"{}\": {:?}",
                    domain_name.to_string(),
                    e
                );

                return None;
            }
        };

        self.self_signed_certs
            .insert(domain_name, self_signed_cert.to_owned());

        Some(self_signed_cert)
    }
}

impl ResolvesServerCert for DynamicCertResolver {
    fn resolve(&self, client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let Some(server_name) = client_hello.server_name() else {
            return None;
        };

        let domain_name = match Domain::<String>::try_from(server_name) {
            Ok(domain_name) => domain_name,
            Err(e) => {
                debug!(
                    "Unable to convert from SNI to a validated DNS domain name: {:?}",
                    e
                );

                return None;
            }
        };

        self.get_or_self_sign(domain_name)
    }
}
