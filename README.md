# airflow-etcd-playground
Playground for orchestration multiple pipelines in Airflow with [etcd](https://etcd.io/) 
*** 
The main goal of this playground is to check possibility of using etcd as ETL 
state store and orchestrating ETL pipelines based on this store.<p>
Distributed systems use etcd as a consistent key-value store for 
configuration management, service discovery, and coordinating distributed work.<p>

Status of this RnD work is PoC.

###### Looks like etcd and watcher needs to be in the separate environment to make it possible to other teams to work with it. For example, adding dependencies.

***
## Code description:
- ```00_watcher.py``` - DAG that used to watch changes in state store. It starts every minute. 
  But for now it works untill it will be turned off. I think that in future it can be 
  constrained with 100 (or any other number) of etcd reads to make logs less heavy and more comfort to check
- ```01_pipline.py``` - initial dummy DAG that runs every minute
- ```02-07_piplines.py``` - dummy DAGs. Their ```schedule_interval=None``` so they can be 
  started only with manual orchestrator
- ```constants.py``` - project constants
- ```etcd_utlis.py``` - procedures that used to work with etcd and to run DAGs via Airflow API
- ```dependencies.json``` - file with pipeline dependencies<p>

Pipeline dependencies in the dependencies.json file looks like:<p>
```json    
{
    "02_pipeline": "01_pipeline",
    "03_pipeline": "01_pipeline",
    "04_pipeline": "01_pipeline",
    "05_pipeline": "01_pipeline",
    "06_pipeline": [
        "02_pipeline",
        "03_pipeline",
        "04_pipeline",
        "05_pipeline"
    ],
    "07_pipeline": "06_pipeline"
}
```
I specified these dependecies in the file because of possibility of versioning. <p>
***
## Installation
Clone repository:

    gh repo clone trifonov-tema/airflow-etcd-playground

Startup environment:

    docker-compose up -d

## Airflow credentials
http://localhost:8080/
- user: airflow 
- password: airflow

## How to test
#### Normal orchestrating
- Enable all DAGs in Airflow 
- See how pipelines are loading one after another regarding the ```dependencies.json``` file

#### Orchestrating DAGs in case of rerunning some DAG Runs
- Enable all DAGs in Airflow 
- Wait till all piplines will be loaded
- Clear DAG Runs for some piplines
- See how related piplines will be loaded regarding  the ```dependencies.json``` file

#### Orchestrating DAGs in case of turned off watcher (due to fail or anything else)
- Enable all DAGs except ```00_watcher``` DAG in Airflow 
- Wait till ```01_pipeline_init``` will be loaded
- See that related DAGs haven't start
- Turn on ```00_watcher``` DAG
- See how related pipelines are loaded regarding the ```dependencies.json``` file



