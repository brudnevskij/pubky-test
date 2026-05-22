use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct Message {
    key: String,
    value: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AggregatedMessage {
    pub key: String,
    pub value: i64,
    pub count: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MessageMetric {
    pub sum: i64,
    pub count: u64,
}

#[derive(Debug, Default)]
pub struct Aggregator {
    state: HashMap<String, MessageMetric>,
}

impl Aggregator {
    pub fn process(&mut self, msg: Message) -> AggregatedMessage {
        let metric = self.state.entry(msg.key.clone()).or_default();

        metric.count += 1;
        metric.sum += msg.value;

        AggregatedMessage {
            key: msg.key,
            value: metric.sum,
            count: metric.count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    proptest! {
        #[test]
        fn running_sum_count_and_avg_are_correct(
            messages in prop::collection::vec(
                ("[a-z]{1,8}", -1_000i64..1_000i64),
                1..200
            )
        ) {
            let mut aggregator = Aggregator::default();
            let mut expected: HashMap<String, (i64, u64)> = HashMap::new();

            for (key, value) in messages {
                let msg = Message {
                    key: key.clone(),
                    value,
                };

                let output = aggregator.process(msg);

                let entry = expected.entry(key.clone()).or_insert((0, 0));
                entry.0 += value;
                entry.1 += 1;

                let expected_sum = entry.0;
                let expected_count = entry.1;

                prop_assert_eq!(output.key, key);
                prop_assert_eq!(output.value, expected_sum);
                prop_assert_eq!(output.count, expected_count);
            }
        }
    }

    proptest! {
        #[test]
        fn different_keys_are_aggregated_independently(
            a_values in prop::collection::vec(-1_000i64..1_000i64, 1..100),
            b_values in prop::collection::vec(-1_000i64..1_000i64, 1..100),
        ) {
            let mut aggregator = Aggregator::default();

            let mut expected_a_sum = 0;
            let mut expected_a_count = 0;

            for value in a_values {
                expected_a_sum += value;
                expected_a_count += 1;

                let output = aggregator.process(Message {
                    key: "cpu".to_string(),
                    value,
                });

                prop_assert_eq!(output.key, "cpu");
                prop_assert_eq!(output.value, expected_a_sum);
                prop_assert_eq!(output.count, expected_a_count);
            }

            let mut expected_b_sum = 0;
            let mut expected_b_count = 0;

            for value in b_values {
                expected_b_sum += value;
                expected_b_count += 1;

                let output = aggregator.process(Message {
                    key: "mem".to_string(),
                    value,
                });

                prop_assert_eq!(output.key, "mem");
                prop_assert_eq!(output.value, expected_b_sum);
                prop_assert_eq!(output.count, expected_b_count);
            }

            let cpu_state = aggregator.state.get("cpu").unwrap();
            let mem_state = aggregator.state.get("mem").unwrap();

            prop_assert_eq!(cpu_state.sum, expected_a_sum);
            prop_assert_eq!(cpu_state.count, expected_a_count);

            prop_assert_eq!(mem_state.sum, expected_b_sum);
            prop_assert_eq!(mem_state.count, expected_b_count);
        }
    }
}
