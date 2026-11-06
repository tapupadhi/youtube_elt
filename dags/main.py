from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datawarehouse.dwh import core_table, staging_table
from dataquality.soda import youtube_elt_data_quality
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "tapupadhi8@gmail.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz)
}

staging_schema = "staging"
core_schema = "core"

# DAG 1: produce_json
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False
) as dag_produce:

    # Define the tasks for the DAG
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(video_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db"
    )

    # Define dependencies between tasks
    playlist_id >> video_ids >> video_data >> save_to_json_task >> trigger_update_db

# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core tables",
    schedule=None,
    catchup=False
) as dag_update:

    # Define the tasks for the DAG
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality"
    )

    # Define dependencies between tasks
    update_staging >> update_core >> trigger_data_quality

# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both the layers in the db",
    schedule=None,
    catchup=False
) as dag_quality:

    # Define the tasks for the DAG
    soda_validate_staging = youtube_elt_data_quality(staging_schema)
    soda_validate_core = youtube_elt_data_quality(core_schema)

    # Define dependencies between tasks
    soda_validate_staging >> soda_validate_core