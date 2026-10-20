from airflow import DAG
from datawarehouse.dwh import core_table, staging_table
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

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule_interval="0 14 * * *",
    catchup=False
) as dag:

    # Define the tasks for the DAG
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(video_data)

    # Define dependencies between tasks
    playlist_id >> video_ids >> video_data >> save_to_json_task

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core tables",
    schedule_interval="0 15 * * *",
    catchup=False
) as dag:

    # Define the tasks for the DAG
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies between tasks
    update_staging >> update_core