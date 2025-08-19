from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_spark_test',
    default_args=default_args,
    description='Teste simples Spark',
    schedule_interval=None,
    catchup=False,
)

# Verificar Spark 
check_spark = BashOperator(
    task_id='check_spark',
    bash_command='curl -f http://spark-master:8080/ || echo "Spark não encontrado"',
    dag=dag,
)

# Executar PySpark
run_pyspark = SparkSubmitOperator(
    task_id='run_pyspark',
    application="/opt/spark-apps/word_count_simple.py",
    conn_id="spark_default",
    dag=dag, 
    verbose=False
)

check_spark >> run_pyspark
