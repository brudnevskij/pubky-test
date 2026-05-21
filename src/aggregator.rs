use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
struct Message {
    key: String,
    value: i64,
}

#[derive(Debug, Clone, Serialize)]
struct AggregatedMessage {
    key: String,
    value: i64,
}

#[derive(Debug, Default)]
struct Aggregator {
    state: HashMap<String, i64>,
}

impl Aggregator {
    fn process(&mut self, msg: Message) -> AggregatedMessage {
        let sum = self.state.entry(msg.key.clone()).or_insert(0);
        *sum += msg.value;

        AggregatedMessage {
            key: msg.key,
            value: *sum,
        }
    }
}
