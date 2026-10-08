use std::{path::PathBuf, sync::Arc};

use dashmap::DashMap;
use hostaddr::Domain;
use tokio::sync::Mutex;

use crate::{
    acme_util::order::OrderService,
    tls_util::{CertAndKey, DynamicCertResolver},
};

pub struct CertificateService {
    certificate_cache_path: PathBuf,
    dynamic_cert_resolver: Arc<DynamicCertResolver>,
    order_service: Arc<OrderService>,
    order_lock: DashMap<Domain<String>, Arc<Mutex<()>>>,
}

impl CertificateService {
    pub fn new(
        certificate_cache_path: PathBuf,
        dynamic_cert_resolver: Arc<DynamicCertResolver>,
        order_service: Arc<OrderService>,
    ) -> Self {
        Self {
            certificate_cache_path: certificate_cache_path,
            dynamic_cert_resolver: dynamic_cert_resolver,
            order_service: order_service,
            order_lock: DashMap::new(),
        }
    }

    pub async fn init_from_disk(&self) -> Result<(), anyhow::Error> {
        let certs = CertAndKey::read_all_from_disk(&self.certificate_cache_path).await?;

        for (domain_name, cert_and_key) in certs {
            self.dynamic_cert_resolver
                .insert(domain_name, cert_and_key.try_into()?);
        }

        Ok(())
    }

    pub async fn order_cert(&self, domain_name: &Domain<String>) -> Result<(), anyhow::Error> {
        if self.dynamic_cert_resolver.get(&domain_name).is_some() {
            return Ok(());
        }

        let order_lock = match self.order_lock.entry(domain_name.to_owned()) {
            dashmap::Entry::Occupied(occupied_entry) => occupied_entry.get().to_owned(),
            dashmap::Entry::Vacant(vacant_entry) => {
                let order_lock = Arc::new(Mutex::new(()));
                vacant_entry.insert(order_lock.to_owned());
                order_lock
            }
        };

        let lock = order_lock.lock().await;

        let cert_and_key = self.order_service.issue_cert(domain_name).await?;
        let certified_key = cert_and_key.clone().try_into()?;

        cert_and_key
            .write_to_disk(&self.certificate_cache_path, &domain_name)
            .await?;

        self.dynamic_cert_resolver
            .insert(domain_name.to_owned(), certified_key);

        drop(lock);

        Ok(())
    }
}
