
# Real-Time Data Aggregation with Tokio & Redis

## How to setup:
You need an active Redis, I have used docker for con tests:

```
docker run --rm --name redis-aggregator-test -p 6379:6379 redis:7
```

## How to run:

```
cargo run --  --redis-url YOUR-REDIS-URL 
```

You can also provide in/out channels with --input/--output



