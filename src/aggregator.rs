use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct Message {
    key: String,
    value: i64,
}

#[derive(Debug)]
struct Aggregator {
    sum: i64,
}

impl Aggregator {
    fn process(&mut self, msg: Message) {
        self.sum += msg.value;
    }
}
