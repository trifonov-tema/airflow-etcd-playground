import logging
from datetime import datetime
import time
import json
from constants import (etcd_host, etcd_port, airflow_host, airflow_password, airflow_username)
import etcd3
from pathlib import Path
from requests.auth import HTTPBasicAuth
import requests
from random import randint

ETCD_ETL_TASK_STATE_PATH = '/etl_state/tasks/'
WATCHED_REVISION = '/watched_revision'


def get_etcd():
    return etcd3.client(host=etcd_host, port=etcd_port)


def work():
    # time.sleep(1)
    time.sleep(randint(0, 20))


def get_key_for_dag(execution_date, dag_name):
    res = ETCD_ETL_TASK_STATE_PATH + execution_date + "/" + dag_name
    return res


def put_state_value(state, **kwargs):
    # start_datetime_nodash = kwargs['ts_nodash']
    # start_date_nodash = kwargs['ds_nodash']
    start_datetime = kwargs['ts']
    start_date = kwargs['ds']
    dag_name = kwargs['dag'].dag_id
    execution_date = kwargs['execution_date']
    key = get_key_for_dag(execution_date.isoformat(), dag_name)
    task_state = {"execution_datetime": execution_date,
                  "execution_date": execution_date.strftime("%Y-%m-%d"),
                  "start_datetime": start_datetime,
                  "dag_name": dag_name,
                  "start_date": start_date,
                  "update_date": datetime.now(),
                  "state": state,
                  "key": key,
                  }
    etcd = get_etcd()
    logging.info(f'WATCHER: PUT Key:{key}, Value:{json.dumps(task_state, default=str)}')
    etcd.put(key, json.dumps(task_state, default=str))


def start_state(**kwargs):
    put_state_value('START', **kwargs)


def end_state(**kwargs):
    put_state_value('END', **kwargs)


def get_dependencies():
    CURRENT_PATH = Path(__file__).parent
    DEPENDENCIES_FILE = Path(CURRENT_PATH, "dependencies.json")
    f = open(DEPENDENCIES_FILE)
    dependencies = json.load(f)
    return dependencies


def get_last_revision():
    return


def etcd_watch_callback(event):
    etcd = get_etcd()

    for e in event.events:
        mod_revision = e.mod_revision
        create_revision = e.create_revision

        key = e.key.decode('utf-8')
        value = json.loads(e.value.decode('utf-8'))

        logging.info(f'WATCHER: MOD REVISION={mod_revision}')
        logging.info(f'WATCHER: CREATE REVISION={create_revision}')

        revision = max(mod_revision, create_revision)

        logging.info(f'WATCHER: CURRENT ETCD REVISION={revision}')
        logging.info(f'WATCHER: Changed Key:{key}, Value:{json.dumps(value, default=str)}')

        state = value["state"]
        execution_datetime = value["execution_datetime"]
        dag_name = value["dag_name"]

        value_name, metadata_names = etcd.get(get_key_for_dag(execution_datetime, dag_name))
        if value_name != None:
            logging.info(f'WATCHER: Change exists in etcd (not deleted)')

            if state == 'END':
                dag_name = value["dag_name"]
                execution_datetime = value["execution_datetime"]
                dependencies = get_dependencies()

                for dependencies_key in dependencies:
                    target_task = dependencies_key
                    source_task = dependencies[dependencies_key]

                    if (dag_name in source_task):
                        if (type(source_task).__name__ == 'str'):
                            logging.info(f'WATCHER: Source DAG ({dag_name}) is completed for {execution_datetime}')
                            logging.info('WATCHER: RUN DAG: ' + target_task)
                            run_dag(target_task, execution_datetime)
                        else:
                            if are_all_source_dags_completed(source_task, execution_datetime) == True:
                                logging.info('WATCHER: RUN DAG: ' + target_task)
                                run_dag(target_task, execution_datetime)
        else:
            logging.info(f'WATCHER: Change NOT exists. Skipping...')

        etcd.put(WATCHED_REVISION, str(revision))


def are_all_source_dags_completed(source_dags, execution_datetime):
    etcd = get_etcd()
    result = True
    for dag_name in source_dags:
        value_name, metadata_names = etcd.get(get_key_for_dag(execution_datetime, dag_name))
        value = json.loads(value_name.decode('utf-8'))

        if (value["state"] != 'END' and value["execution_datetime"] == execution_datetime):
            result = False
            break

    logging.info(f'WATCHER: All source DAGs ({source_dags}) are completed for {execution_datetime}')
    return result


def run_dag(dag_name, execution_datetime):
    get_dag_result = request_airflow_api_get_dag_run(execution_datetime, dag_name)
    check_get_dag_request(get_dag_result.status_code, dag_name)

    if get_dag_result.status_code == 200:
        clear_dag_result = request_airflow_api_clear_dag_run(execution_datetime, dag_name)
        check_clear_dag_request(get_dag_result.status_code, dag_name)
        result_status_code = clear_dag_result.status_code

    elif get_dag_result.status_code == 404:
        run_dag_result = request_airflow_api_run_dag(execution_datetime, dag_name)
        check_dag_run_request(get_dag_result.status_code, dag_name)
        result_status_code = run_dag_result.status_code


def check_dag_run_request(status_code, dag_name):
    if status_code == 200:
        logging.info('WATCHER: DAG ' + dag_name + ' successfully started')
    elif status_code == 400:
        logging.error('WATCHER: ERROR. DAG run. DAG: ' + dag_name + '. 400 - Client specified an invalid argument.')
    elif status_code == 401:
        logging.error(
            'WATCHER: ERROR. DAG run. DAG: ' + dag_name + '. 401 - Request not authenticated due to missing, invalid, authentication info.')
    elif status_code == 403:
        logging.error(
            'WATCHER: ERROR. DAG run. DAG: ' + dag_name + '. 403 - Client does not have sufficient permission.')
    elif status_code == 404:
        logging.error('WATCHER: ERROR. DAG run. DAG: ' + dag_name + '. 404 - A specified resource is not found.')
    elif status_code == 409:
        logging.error(
            'WATCHER: ERROR. DAG run. DAG: ' + dag_name + '. 409 - An existing resource conflicts with the request.')


def check_clear_dag_request(status_code, dag_name):
    if status_code == 200:
        logging.info('WATCHER: DAG ' + dag_name + ' successfully CLEARED')
    elif status_code == 400:
        logging.error('WATCHER: ERROR. DAG CLEAR. DAG: ' + dag_name + '. 400 - Client specified an invalid argument.')
    elif status_code == 401:
        logging.error(
            'WATCHER: ERROR. DAG CLEAR. DAG: ' + dag_name + '. 401 - Request not authenticated due to missing, invalid, authentication info.')
    elif status_code == 403:
        logging.error(
            'WATCHER: ERROR. DAG CLEAR. DAG: ' + dag_name + '. 403 - Client does not have sufficient permission.')
    elif status_code == 404:
        logging.error('WATCHER: ERROR. DAG CLEAR. DAG: ' + dag_name + '. 404 - A specified resource is not found.')


def check_get_dag_request(status_code, dag_name):
    if status_code == 200:
        logging.info('WATCHER: DAG ' + dag_name + ' successfully pulled')
    elif status_code == 401:
        logging.error(
            'WATCHER: ERROR. DAG CLEAR. DAG: ' + dag_name + '. 401 - Request not authenticated due to missing, invalid, authentication info.')
    elif status_code == 403:
        logging.error(
            'WATCHER: ERROR. DAG CLEAR. DAG: ' + dag_name + '. 403 - Client does not have sufficient permission.')
    elif status_code == 404:
        logging.error('WATCHER: ERROR. DAG CLEAR. DAG: ' + dag_name + '. 404 - A specified resource is not found.')


def get_dag_run_id(dag_name, execution_datetime):
    return f'manual_api_{execution_datetime}_{dag_name}'


def request_airflow_api_get_dag_run(execution_datetime, dag_name):
    url = f'http://{airflow_host}/api/v1/dags/{dag_name}/dagRuns/{get_dag_run_id(dag_name, execution_datetime)}'
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    logging.info(f'WATCHER: Trying to GET the DAG {dag_name} for the date {execution_datetime}')

    result = requests.get(
        url,
        headers=headers,
        auth=HTTPBasicAuth(airflow_username, airflow_password)
    )
    return result


def request_airflow_api_run_dag(execution_datetime, dag_name):
    url = f'http://{airflow_host}/api/v1/dags/{dag_name}/dagRuns'
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = {
        'dag_run_id': get_dag_run_id(dag_name, execution_datetime),
        'logical_date': execution_datetime,
    }
    logging.info(f'WATCHER: Trying to RUN the DAG {dag_name} for the date {execution_datetime}')

    result = requests.post(
        url,
        json=data,
        headers=headers,
        auth=HTTPBasicAuth(airflow_username, airflow_password)
    )
    return result


def request_airflow_api_clear_dag_run(execution_datetime, dag_name):
    dag_run_id = get_dag_run_id(dag_name, execution_datetime)
    url = f'http://{airflow_host}/api/v1/dags/{dag_name}/dagRuns/{dag_run_id}/clear'
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = {"dry_run": False}
    logging.info(f'WATCHER: Trying to CLEAR the DAG {dag_name} for the date {execution_datetime}')

    result = requests.post(
        url,
        json=data,
        headers=headers,
        auth=HTTPBasicAuth(airflow_username, airflow_password)
    )
    return result
