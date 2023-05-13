# from airflow.decorators import dag, task
import airflow
from src.ops import retrieve_redis_data, \
    redis_time_series_to_dataframe, \
    dump_dataframe_to_postgres, remove_redis_cache_data
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG

# conf = {"spark.master": master, "spark.app.name": "StreamProcessingDemo",

with DAG(
        'process_data_for_stream_processing',
        schedule_interval=timedelta(minutes=20),
        start_date=airflow.utils.dates.days_ago(2),
        max_active_runs=1,
        is_paused_upon_creation=False,
        catchup=False
) as dag:
    get_redis_data_task = PythonOperator(
        task_id='get_stream_data',
        python_callable=retrieve_redis_data,
        op_kwargs={
            "data_type": "unq_dst_ip"
        },
        do_xcom_push=True,
        dag=dag
    )

    redis_to_dataframe_task = PythonOperator(
        task_id="convert_to_dataframe",
        python_callable=redis_time_series_to_dataframe,
        op_kwargs={
            "train_type": "unq_dst_ip"
        },
        do_xcom_push=True,
        dag=dag
    )

    dump_to_database_task = PythonOperator(
        task_id="dump_redis_data_to_postgres",
        python_callable=dump_dataframe_to_postgres,
        dag=dag
    )

    delete_cache_task = PythonOperator(
        task_id="clean_redis_data",
        python_callable=remove_redis_cache_data,
        dag=dag
    )

    get_redis_data_task >> redis_to_dataframe_task >> dump_to_database_task >> delete_cache_task
