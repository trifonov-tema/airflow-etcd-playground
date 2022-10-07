FROM apache/airflow:2.4.1
RUN pip install etcd3
RUN pip install requests