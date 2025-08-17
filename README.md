# GTFS to Kafka

Create an env file with the following parameters:
- KAFKA_BOOTSTRAP
- KAFKA_USER
- KAFKA_PASSWORD

Use that env file in the following:
```
docker build -t gtfs2kafka:0.0 .
docker run --env-file path/to/1.env gtfs2kafka:0.0
```


