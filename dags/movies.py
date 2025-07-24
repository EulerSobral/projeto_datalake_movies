from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG("movie_list", start_date=datetime(2025,1,1), schedule_interval=None) as dag:
    
    bronze = SparkSubmitOperator(
        task_id="bronze",
        application="/opt/spark-apps/bronze_spark.py",
        conn_id="spark_default", 
        executor_memory='4g', 
        total_executor_cores='2',
        executor_cores='2',
        num_executors='2',
        driver_memory='2g', 
        conf={
        "spark.network.timeout": "300s",
        "spark.executor.heartbeatInterval": "60s"
        },  
        verbose=True
    )

    silver = SparkSubmitOperator(
        task_id="silver",
        application="/opt/spark-apps/silver_spark.py",
        conn_id="spark_default",
        executor_memory='4g',       # Aumentar memória por executor
        total_executor_cores='4',   # Total de cores para o job
        executor_cores='2',         # 2 cores por executor
        num_executors='2',          
        driver_memory='2g',         
        conf={
            "spark.network.timeout": "300s",  
            "spark.executor.heartbeatInterval": "60s"
        },
        verbose=True
    )
    
    gold = SparkSubmitOperator(
        task_id="gold",
        application="/opt/spark-apps/gold_spark.py",
        conn_id="spark_default",
        executor_memory='4g',       # Aumentar memória por executor
        total_executor_cores='4',   # Total de cores para o job
        executor_cores='2',         # 2 cores por executor
        num_executors='2',          
        driver_memory='2g',         
        conf={
            "spark.network.timeout": "300s",  
            "spark.executor.heartbeatInterval": "60s"
        },
        verbose=True
    )
    
    
    bronze >> silver >> gold