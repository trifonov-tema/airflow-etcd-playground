from airflow import DAG
from etcd_utils import (start_state, work, end_state)
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 10, 5),
}

dag = DAG('05_pipeline',
          schedule_interval=None,
          default_args=args,
          tags=['tema'],
          concurrency=1,
          max_active_runs=1,
          )

start = PythonOperator(
    task_id='start',
    provide_context=True,
    python_callable=start_state,
    dag=dag,
)

work = PythonOperator(
    task_id='work',
    provide_context=True,
    python_callable=work,
    dag=dag,
)

end = PythonOperator(
    task_id='end',
    provide_context=True,
    python_callable=end_state,
    dag=dag,
)

start >> work >> end
