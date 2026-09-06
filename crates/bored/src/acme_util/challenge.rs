use dashmap::DashMap;

pub struct ChallengeService {
    challenges: DashMap<String, String>,
}

impl ChallengeService {
    pub fn new() -> Self {
        Self {
            challenges: DashMap::new(),
        }
    }

    pub fn accept_challenge(&self, token: String, key_authorization: String) {
        self.challenges.insert(token, key_authorization);
    }

    pub fn complete_challenge(&self, token: &str) -> Option<String> {
        self.challenges
            .get(token)
            .map(|key_authorization| key_authorization.to_owned())
    }

    pub fn decline_challenge(&self, token: &str) {
        self.challenges.remove(token);
    }
}
