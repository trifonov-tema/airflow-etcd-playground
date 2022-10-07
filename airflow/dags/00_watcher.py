from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time
import logging
from datetime import datetime
from etcd_utils import (etcd_watch_callback, get_etcd, ETCD_ETL_TASK_STATE_PATH, WATCHED_REVISION)


def etcd_watcher(**kwargs):
    etcd = get_etcd()
    value, meta = etcd.get(WATCHED_REVISION)

    if value == None:
        revision = 1
    else:
        revision = int(value) + 1

    logging.info(f'WATCHER: STARTING WATCHER REVISION = {revision}')

    etcd.add_watch_prefix_callback(ETCD_ETL_TASK_STATE_PATH, etcd_watch_callback, start_revision=revision)

    while True:
        time.sleep(1)

    return result


args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 10, 5),
}

# %%
dag = DAG('00_watcher',
          schedule_interval='* * * * *',
          default_args=args,
          tags=['tema'],
          concurrency=1,
          max_active_runs=1,
          )

etcd_watcher = PythonOperator(
    task_id='etcd_watcher',
    provide_context=True,
    python_callable=etcd_watcher,
    dag=dag,
)

# %%
