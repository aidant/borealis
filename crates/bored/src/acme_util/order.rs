use std::sync::Arc;

use hostaddr::Domain;
use instant_acme::{
    AuthorizationStatus, ChallengeType, Identifier, NewOrder, OrderStatus, RetryPolicy,
};

use crate::{
    acme_util::{account::AccountService, challenge::ChallengeService},
    tls_util::CertAndKey,
};

pub struct OrderService {
    account_service: Arc<AccountService>,
    challenge_service: Arc<ChallengeService>,
}

#[derive(Debug, thiserror::Error)]
pub enum IssueCertError {
    #[error("no account has been created")]
    NoAccount,

    #[error("ACME order failed: {0}")]
    OrderFailed(#[source] instant_acme::Error),

    #[error("ACME order authorisation failed: {0}")]
    Authorization(#[source] instant_acme::Error),

    #[error("ACME order authorisation status invalid")]
    InvalidAuthorizationStatus(AuthorizationStatus),

    #[error("ACME order challenge unsupported")]
    UnsupportedChallenge,

    #[error("ACME order challenge ready failed: {0}")]
    ChallengeSetReady(#[source] instant_acme::Error),

    #[error("ACME order challenge poll failed: {0}")]
    ChallengePollReady(#[source] instant_acme::Error),

    #[error("ACME order is still pending")]
    OrderPending,

    #[error("ACME order is invalid")]
    OrderInvalid,

    #[error("ACME order is in an infailable state of: {0:?}")]
    OrderStatusNever(OrderStatus),

    #[error("ACME order finilisation failed: {0}")]
    OrderFinalize(#[source] instant_acme::Error),

    #[error("ACME order certificate poll failed: {0}")]
    OrderPollCertificate(#[source] instant_acme::Error),
}

impl OrderService {
    pub fn new(
        account_service: Arc<AccountService>,
        challenge_service: Arc<ChallengeService>,
    ) -> Self {
        Self {
            account_service: account_service,
            challenge_service: challenge_service,
        }
    }

    pub(super) async fn issue_cert(
        &self,
        domain_name: &Domain<String>,
    ) -> Result<CertAndKey, IssueCertError> {
        let account = self
            .account_service
            .get()
            .ok_or(IssueCertError::NoAccount)?;

        let mut order = account
            .new_order(&NewOrder::new(&[Identifier::Dns(domain_name.to_string())]))
            .await
            .map_err(IssueCertError::OrderFailed)?;

        let mut challenges = Vec::new();
        let mut authorizations = order.authorizations();

        while let Some(authorization_result) = authorizations.next().await {
            let mut authorization = authorization_result.map_err(IssueCertError::Authorization)?;

            match authorization.status {
                AuthorizationStatus::Pending => {}
                AuthorizationStatus::Valid => continue,
                status => return Err(IssueCertError::InvalidAuthorizationStatus(status)),
            };

            let mut challenge = authorization
                .challenge(ChallengeType::Http01)
                .ok_or(IssueCertError::UnsupportedChallenge)?;

            challenges.push(challenge.token.to_owned());

            self.challenge_service.accept_challenge(
                challenge.token.to_owned(),
                challenge.key_authorization().as_str().to_owned(),
            );

            challenge
                .set_ready()
                .await
                .map_err(IssueCertError::ChallengeSetReady)?;
        }

        match order
            .poll_ready(&RetryPolicy::default())
            .await
            .map_err(IssueCertError::ChallengePollReady)?
        {
            OrderStatus::Pending => return Err(IssueCertError::OrderPending),
            OrderStatus::Ready => {}
            OrderStatus::Invalid => return Err(IssueCertError::OrderInvalid),
            status => return Err(IssueCertError::OrderStatusNever(status)),
        };

        let signing_key_pem = order
            .finalize()
            .await
            .map_err(IssueCertError::OrderFinalize)?;

        let cert_chain_pem = order
            .poll_certificate(&RetryPolicy::default())
            .await
            .map_err(IssueCertError::OrderPollCertificate)?;

        Ok(CertAndKey {
            cert: cert_chain_pem,
            key: signing_key_pem,
        })
    }
}
