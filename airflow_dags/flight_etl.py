from datetime import datetime
from datetime import timedelta
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def call_procedure(procedure_name):
    hook = PostgresHook(postgres_conn_id="depi_db")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try: 
        print(f"Starting {procedure_name} procedure execution...")
        cursor.execute(f"CALL {procedure_name}_layer();")

        conn.commit()
        print(f"Procedure {procedure_name} execution is done...")
    except Exception as e:
        conn.rollback()
        print(f"Procedure {procedure_name} failed: {e}")
        raise

    finally: 
        cursor.close()
        conn.close()

def run_talend_job():
    result = subprocess.run([
        '/bin/bash', '-c', 
        'cd /opt/airflow/talend_jobs/parent_job && PATH=/usr/bin:$PATH ./parent_job_run.sh'
    ], capture_output=True, text=True)
    
    
    if result.returncode != 0:
        raise Exception(f"Talend job failed: {result.stderr}")


with DAG(
    dag_id="flight_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    kafka_data_sender = BashOperator(
            task_id='kafka_data_sender',
            bash_command=r'''
                docker exec -it --user root spark-notebook /usr/local/spark/bin/spark-submit \
                    --master spark://spark-master:7077 \
                    --name NotebookStreamingConsumer \
                    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
                    /tmp/kafka_streaming_job.py
            ''',
            execution_timeout=timedelta(minutes=5) 
    )
    
    spark_batch_processor = BashOperator(
        task_id='spark_batch_processor',
        bash_command=r'''
            docker exec -it spark-master /spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --jars /tmp/spark_drivers/postgresql-42.7.7.jar \
                /opt/create_tables.py
        ''',
        execution_timeout=timedelta(minutes=10) 
    )
    
    spark_streaming_start = BashOperator(
        task_id='spark_streaming_start',
        bash_command=r'''
            docker exec -d spark-master /spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
                --jars /opt/spark/jars/postgresql-42.7.7.jar \
                /opt/kafka_consumer.py
        ''',
        execution_timeout=timedelta(seconds=15) 
    )
    
    call_bronze_procedure = PythonOperator(
        task_id="call_bronze_procedure",
        python_callable=call_procedure,
        op_args=['bronze']
    )

    call_silver_procedure = PythonOperator(
        task_id="call_silver_procedure",
        python_callable=call_procedure,
        op_args=['silver']
    )

    call_gold_procedure = PythonOperator(
        task_id="call_gold_procedure",
        python_callable=call_procedure,
        op_args=['gold']
    )

    run_python_script = BashOperator(
        task_id = 'python_etl_pipeline',
        bash_command='cd /opt/airflow/etl_scripts && pip install -r requirements.txt && python main.py'
    )


    run_talend_etl = PythonOperator(
        task_id="talend_etl_pipeline",
        python_callable=run_talend_job,
    )
    [kafka_data_sender, spark_streaming_start, spark_batch_processor] >> call_bronze_procedure
    
    (
        call_bronze_procedure 
        >> run_python_script 
        >> run_talend_etl 
        >> call_silver_procedure
        >> call_gold_procedure
    )

    