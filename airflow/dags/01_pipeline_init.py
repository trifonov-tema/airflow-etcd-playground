from airflow import DAG
from etcd_utils import (start_state, work, end_state)
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 10, 5),
    'end_date': datetime(2022, 10, 5, 0, 4)
}

dag = DAG('01_pipeline_init',
          schedule_interval='* * * * *',
          default_args=args,
          tags=['tema'],
          concurrency=3,
          max_active_runs=3,
          catchup=True,
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
