from airflow import DAG
import pendulum
from datetime import timedelta
from api.video_stats import get_channel_playlistId, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": "data@engineers.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": pendulum.now(local_tz)
    # "end_date": pendulum.datetime(2024, 6, 30, tz=local_tz)
}
# Variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: produce_json
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG untuk mengambil data dari YouTube API dan menyimpannya dalam file JSON",
    schedule="10 0 * * *",  # Menjadwalkan DAG untuk berjalan setiap hari pada pukul 00:00
    catchup=False,
) as dag:
    # mendefinisikan task untuk mendapatkan playlistId dari channel YouTube
    playlistId = get_channel_playlistId()
    video_ids = get_video_ids(playlistId)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    # Menentukan urutan eksekusi task
    playlistId >> video_ids >> extracted_data >> save_to_json_task

# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG untuk memproses data dari file JSON ke kedua schema staging dan production di database PostgreSQL",
    schedule="20 0 * * *",  # Menjadwalkan DAG untuk berjalan setiap hari pada pukul 00:00
    catchup=False,
) as dag:
    # mendefinisikan task untuk mendapatkan playlistId dari channel YouTube
    update_staging = staging_table()
    update_core = core_table()

    # Menentukan urutan eksekusi task
    update_staging >> update_core

# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core