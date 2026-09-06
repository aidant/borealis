use std::path::PathBuf;

pub struct CertificateService {
    certificate_cache_path: PathBuf,
}

impl CertificateService {
    pub fn new(certificate_cache_path: PathBuf) -> Self {
        Self {
            certificate_cache_path: certificate_cache_path,
        }
    }
}
