
# Real-Time Data Aggregation with tokio & redis

## Setup:
You need an active redis, use docker:

```bash
docker run --rm --name redis-aggregator-test -p 6379:6379 redis:7
```

Then you can publish messages by running:

```bash
docker exec -it redis-aggregator-test redis-cli PUBLISH inputA '{"key":"cpu","value":10}'

```

```bash
docker exec -it redis-aggregator-test redis-cli PUBLISH inputB '{"key":"cpu","value":10}'

```

```bash
docker exec -it redis-aggregator-test redis-cli PUBLISH inputC '{"key":"cpu","value":10}'

```

To listen to output channel:
```bash
docker exec -it redis-aggregator-test redis-cli SUBSCRIBE outputChannel
```

## How to run:

Assuming redis is listening on 127.0.0.1:6379:
```bash
cargo run

```
If you want modify startup:
```bash
cargo run --  --redis-url <YOUR-REDIS-URL> --inputs <CHANNEL-A>,<CHANNEL-B>,<CHANNEL-C> --output <OUTPUT-CHANNEL> 
```

## Tests

If you are feeling lazy to test `./tests/redis_pipeline.rs` contains end-to-end example of the application, using test-containers.
Also, `./src/aggregator.rs` contains property tests for the message Aggregator.  

To run: 
```
cargo test 
```

