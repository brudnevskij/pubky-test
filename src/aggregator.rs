use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct Message {
    key: String,
    value: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AggregatedMessage {
    key: String,
    value: i64,
}

#[derive(Debug, Default)]
pub struct Aggregator {
    state: HashMap<String, i64>,
}

impl Aggregator {
    pub fn process(&mut self, msg: Message) -> AggregatedMessage {
        let sum = self.state.entry(msg.key.clone()).or_insert(0);
        *sum += msg.value;

        AggregatedMessage {
            key: msg.key,
            value: *sum,
        }
    }
}
