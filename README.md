# README

This repo demonstrates use of durable orchestration azure function.
With this, you have an advanced structure of ETL orchestration using azure functions.

Here you will find usage of:

* Clean Architecture
* Data Validation
* Function Chaining in ETL Orchestration

## Orchestration with azure function recipe

![ETL With Azure Function Pattern](./docs/task_orchestration_pattern.png?raw=true "ETL Pattern")

### Note

Orchestration is one of the main challenges of the ingestion. 

Most of the extraction, processing and loading operation we perform require multiple data storages, in-between saving intermediate data or consuming external services (such as Azure Machine Learning Service).

We can summarize the orchestration scheme as:

1. Trigger Orchestrator
2. Orchestrator function starts
3. For each subtask, orchestration logs state, schedules tasks, once completed, logs state


## Pre-requisities

Make sure you have:

* Python 
* Poetry (installation instructions [here](https://python-poetry.org/docs/#installation))
* Docker container manager (use [colima](https://github.com/abiosoft/colima#installation) for macOs)

Optional:

If you are using Code as your Code editor you can install the following extensions:
* [Durable Functions Monitor](https://marketplace.visualstudio.com/items?itemName=DurableFunctionsMonitor.durablefunctionsmonitor)

## Setup local env

You can run:

```sh
make init-env-file # Copy default ./docker/template.env to ./docker/.env

make setup # Install project dependencies
```

You can then run an orchestration for demonstration purpose by doing the following:

```sh
colima start # Start docker service if you are using colima
make start-dev 
make check-up-azfn # To check if azure function hub is up and running
```

At this point you can monitor the orchestration using the durable functions monitor vscode extension.
The connection string should be: 
```
DefaultEndpointsProtocol=http;AccountName=localstoreaccount;AccountKey=key1;BlobEndpoint=http://localhost:10000/localstoreaccount;QueueEndpoint=http://localhost:10001/localstoreaccount;TableEndpoint=http://localhost:10002/localstoreaccount;
```

Then to trigger an orchestration, do the following:

```sh
curl http://localhost:8080/api/orchestrator
```

## Run tests

For unit tests
```sh
make run-unit-tests # Check if unit tests work
```

For integration tests:

```sh
make start-dev # To get local env running
make check-up-azfn # To check if azure function hub is up and running
make run-integration-tests
make stop-dev # To shutdown local env 
```
### Project structure

```
.
├── README.md              <- The top-level README for developers using this project.
├── Makefile               <- Makefile with commands like `make setup` to install the project
├── docker                 <- Docker Configuration to run a local environment           
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── template.env       <- Template of .env file to use for docker configuration
│   └── wait-azfn.sh
├── function_apps          <- Folder containing all the azure functions
├── py_project             <- Python project to import in azure functions
├── pyproject.toml         <- Build poetry configuration holding dependencies and tools configurations
├── poetry.lock
└── tests                  <- Tests
    ├── integration
    └── unit
```

### References

* [Azure functions docker python sample](https://github.com/Azure/azure-functions-docker-python-sample)
* [Unit tests sample for durable functions](https://github.com/kemurayama/durable-functions-for-python-unittest-sample)
* [Durable function monitor extension](https://github.com/microsoft/DurableFunctionsMonitor/wiki)
* [Issue when running func host start](https://github.com/Azure/azure-functions-core-tools/issues/3042)
* [Kaggle Weather dataset](https://www.kaggle.com/datasets/muthuj7/weather-dataset/code)
* [Best Practices with Flyway](https://dbabulletin.com/index.php/2018/03/29/best-practices-using-flyway-for-database-migrations/)


