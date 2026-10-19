use super::{Fault, Outcome, commit};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Token {
    pub generation: u8,
    pub assignment: u8,
    pub attempt: u8,
}

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Subscription {
    pub generation: u8,
    pub assignment: u8,
    pub attempt: u8,
    pub acknowledged: bool,
    pub delayed: Option<Token>,
}

impl Subscription {
    pub fn token(&self) -> Option<Token> {
        (self.attempt > 0).then_some(Token {
            generation: self.generation,
            assignment: self.assignment,
            attempt: self.attempt,
        })
    }

    pub fn deliver(&mut self, fault: Fault) -> Outcome {
        commit(self, fault, |state| {
            if state.attempt >= 2 || state.acknowledged {
                return false;
            }
            state.attempt += 1;
            true
        })
    }

    pub fn acknowledge(&mut self, token: Token, fault: Fault) -> Outcome {
        commit(self, fault, |state| {
            if state.token() != Some(token) {
                return false;
            }
            state.acknowledged = true;
            true
        })
    }

    pub fn seek(&mut self, fault: Fault) -> Outcome {
        commit(self, fault, |state| {
            if state.generation >= 2 {
                return false;
            }
            state.generation += 1;
            state.attempt = 0;
            state.acknowledged = false;
            true
        })
    }

    pub fn reassign(&mut self, fault: Fault) -> Outcome {
        commit(self, fault, |state| {
            if state.assignment >= 2 {
                return false;
            }
            state.assignment += 1;
            state.attempt = 0;
            true
        })
    }
}
