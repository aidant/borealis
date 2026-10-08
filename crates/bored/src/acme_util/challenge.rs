use dashmap::DashMap;

pub struct ChallengeService {
    challenges: DashMap<String, String>,
}

pub struct ScopedChallenges<'a> {
    accepted_challenges: Vec<String>,
    challenge_service: &'a ChallengeService,
}

impl ChallengeService {
    pub fn new() -> Self {
        Self {
            challenges: DashMap::new(),
        }
    }

    pub fn with_scoped_challenges<'a>(&'a self) -> ScopedChallenges<'a> {
        ScopedChallenges {
            accepted_challenges: Vec::new(),
            challenge_service: &self,
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

impl<'a> ScopedChallenges<'a> {
    pub fn accept_challenge(&mut self, token: String, key_authorization: String) {
        self.accepted_challenges.push(token.clone());
        self.challenge_service
            .accept_challenge(token, key_authorization);
    }
}

impl<'a> Drop for ScopedChallenges<'a> {
    fn drop(&mut self) {
        for token in &self.accepted_challenges {
            self.challenge_service.decline_challenge(token);
        }
    }
}
