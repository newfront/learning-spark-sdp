# learning-spark-sdp
This is environment created to build up the mental muscle to master the craft of Spark Declarative Pipelines (SDP).

> Note: This project represents my own journey from Apache Spark Structured Streaming (SS) to SDP. 
> If you don't know this, I was so enamoured with SS that I wrote an entire book on the subject [Modern Data Engineering with Apache Spark](https://www.oreilly.com/library/view/modern-data-engineering/9781484274521/). It was written entirely in Scala. This project is written entirely in Python and Spark SQL, given the primatives for SDP are exactly that.

## Local Spark Cluster

The `docker-compose.yaml` spins up a standalone Apache Spark 4.1.1 cluster with one master and three workers.

### Requirements

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [just](https://just.systems/) — command runner (replaces `make`)

  ```bash
  brew install just
  ```

- [pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation) — Python version manager

  ```bash
  brew install pyenv
  ```

- [jenv](https://www.jenv.be/) — Java version manager

  ```bash
  brew install jenv
  ```

  Add to your shell config (`~/.zshrc` or `~/.bash_profile`):

  ```bash
  echo 'eval "$(jenv init -)"' >> ~/.zshrc
  ```

  Enable the `export` plugin so `JAVA_HOME` is set automatically:

  ```bash
  jenv enable-plugin export
  ```

- [Astral uv](https://docs.astral.sh/uv/getting-started/installation/) — Python package and project manager

  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

### Quick start (with just)

```bash
just cluster-up                                      # start the cluster
just sdp-run sdp-playground/example_pipeline         # run the example pipeline
just cluster-down                                    # tear it down
```

Run `just` (or `just --list`) at any time to see all available recipes.

### Cluster web UIs

| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Worker 1 | http://localhost:8081 |
| Worker 2 | http://localhost:8082 |
| Worker 3 | http://localhost:8083 |

### Manual cluster commands

```bash
docker compose up -d      # start
docker compose ps         # status
docker compose logs -f    # tail logs
docker compose down       # stop
```

## SDP Playground

The [`sdp-playground/`](./sdp-playground) directory is a `uv`-managed Python
project for learning [Spark Declarative Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
on PySpark 4.1.1. It contains an example pipeline (Python + SQL materialized
views) and pytest-based tests for the underlying transformation logic. See
[`sdp-playground/README.md`](./sdp-playground/README.md) for usage details.
