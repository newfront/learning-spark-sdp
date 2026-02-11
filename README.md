# learning-spark-sdp
This is an entire environment created to master the craft of Spark Declarative Pipelines.

## Local Spark Cluster

The `docker-compose.yaml` spins up a standalone Apache Spark 4.1.1 cluster with one master and three workers.

### Requirements

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)

### Start the cluster

```bash
docker compose up -d
```

### Web UIs

| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Worker 1 | http://localhost:8081 |
| Worker 2 | http://localhost:8082 |
| Worker 3 | http://localhost:8083 |

### Submit a job

```bash
spark-submit --master spark://localhost:7077 <your-job.py>
```

Or from within a container:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  <your-job.py>
```

### Stop the cluster

```bash
docker compose down
```
