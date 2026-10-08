use std::{path::PathBuf, sync::Arc};

use instant_acme::{Account, AccountCredentials, NewAccount};
use tokio::{fs, sync::Mutex};

pub struct AccountService {
    account_credentials_path: PathBuf,
    account: Arc<Mutex<Option<Account>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountFromCredentialsError {
    #[error("read from disk error: {0}")]
    ReadFromDisk(#[from] std::io::Error),

    #[error("deserialisation error: {0}")]
    Deserialisation(#[from] serde_json::Error),

    #[error("account error: {0}")]
    Account(#[from] instant_acme::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum AccountCreateError {
    #[error("account error: {0}")]
    Account(#[from] instant_acme::Error),

    #[error("serialisation error: {0}")]
    Serialisation(#[from] serde_json::Error),

    #[error("write to disk error: {0}")]
    WriteToDisk(#[from] std::io::Error),
}

impl AccountService {
    pub fn new(account_credentials_path: PathBuf) -> Self {
        Self {
            account_credentials_path: account_credentials_path,
            account: Arc::new(Mutex::new(None)),
        }
    }

    pub fn get(&self) -> Option<Account> {
        self.account
            .try_lock()
            .ok()
            .and_then(|account| account.to_owned())
    }

    pub async fn init_from_disk(&self) -> Result<(), AccountFromCredentialsError> {
        let account_credentials_string = fs::read_to_string(&self.account_credentials_path).await?;
        let account_credentials =
            serde_json::from_str::<AccountCredentials>(&account_credentials_string)?;

        let account = Account::builder()?
            .from_credentials(account_credentials)
            .await?;

        self.account.lock().await.insert(account);

        Ok(())
    }

    pub async fn create(
        &self,
        email: &str,
        tos: bool,
        directory_url: &str,
    ) -> Result<(), AccountCreateError> {
        let (account, account_credentials) = Account::builder()?
            .create(
                &NewAccount {
                    contact: &[format!("mailto:{}", email).as_str()],
                    terms_of_service_agreed: tos,
                    only_return_existing: false,
                },
                directory_url.to_owned(),
                None,
            )
            .await?;

        let account_credentials_string = serde_json::to_string_pretty(&account_credentials)?;

        fs::write(&self.account_credentials_path, account_credentials_string).await?;

        self.account.lock().await.insert(account);

        Ok(())
    }
}
