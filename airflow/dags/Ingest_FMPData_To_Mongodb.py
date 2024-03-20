from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from InvestmentAsCode_AssetFlowPlatform.jobs import store_company_general_info
from InvestmentAsCode_AssetFlowPlatform.jobs import store_etf_list

schedule_interval = "0 15 * * *"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 5),
}

with DAG(
    "Ingest_FMPData_To_Mongodb",
    default_args=default_args,
    schedule_interval=schedule_interval,
) as dag:

    task_1 = PythonOperator(
        task_id="store_company_general_info",
        python_callable=store_company_general_info.task,
    )

    task_2 = PythonOperator(
        task_id="store_etf_list",
        python_callable=store_etf_list.task,
    )

    task_1 >> task_2
