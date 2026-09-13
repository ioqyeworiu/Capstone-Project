from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from datetime import datetime
from zoneinfo import ZoneInfo

timezone = ZoneInfo("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="daily_load_warehouse",
    start_date=datetime(2026, 9, 9, tzinfo=timezone),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    
    start = BashOperator(
        task_id="start",
        bash_command="echo 'Start daily load warehouse'",
    )
    
    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command="bash job_script/daily_load_warehouse_script.sh {{ ds }}",   
        cwd="/home/pmquanbackup/data/airflow"
    )
    
    end = BashOperator(
        task_id="end",
        bash_command="echo 'End daily load warehouse'",
    )
    
    start >> load_warehouse >> end