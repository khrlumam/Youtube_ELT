# Tujuan dari file ini adalah untuk melakukan proses staging data dari file JSON ke tabel staging di database PostgreSQL. Proses staging ini melibatkan beberapa langkah, yaitu:
# 1. Membuat schema dan tabel staging di database PostgreSQL jika belum ada.
# 2. Melakukan insert atau update data ke tabel staging sesuai dengan video_id yang ada di tabel.
# 3. Melakukan delete pada row yang memiliki video_id yang tidak ada di file JSON.
# 4. Menyimpan log proses staging data ke dalam file log.

from datawarehouse.data_utils import get_conn_cursor, create_schema, create_table, get_video_ids, close_conn_cursor
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
# Fungsi untuk melakukan proses staging data dari file JSON ke tabel staging di database PostgreSQL
def staging_table():
    schema = "staging"

    conn, cursor = None, None

    try:
        conn, cursor = get_conn_cursor()
        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cursor, schema)

        # Melakukan iterasi pada setiap row data yang telah diekstrak untuk melakukan insert atau update ke tabel staging sesuai dengan video_id yang ada di tabel
        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cursor, conn, schema, row)
            
            else:
                if row["video_id"] in table_ids:
                    update_rows(cursor, conn, schema, row)
                else:
                    insert_rows(cursor, conn, schema, row)

        # Melakukan delete pada row yang memiliki video_id yang tidak ada di file JSON tetapi ada di tabel staging
        ids_in_json = {row["video_id"] for row in YT_data}

        ids_to_delete = set(table_ids) - ids_in_json

        # Untuk menghindari error saat melakukan delete pada row yang memiliki video_id yang tidak ada di file JSON tetapi ada di tabel staging, 
        # maka dilakukan pengecekan terlebih dahulu apakah ada video_id yang akan dihapus atau tidak
        if ids_to_delete:
            delete_rows(cursor, conn, schema, ids_to_delete)

        logger.info(f"Staging table {schema}.{table} updated successfully.")

    except Exception as e:
        logger.error(f"Error updating staging table {schema}.{table}: {e}")
        raise e

    finally:
        if cursor and conn:
            close_conn_cursor(conn, cursor)

@task
def core_table():
    # Fungsi untuk melakukan proses staging data dari tabel staging ke tabel production di database PostgreSQL
    schema = "core"

    conn, cursor = None, None

    try :
        conn, cursor = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cursor, schema)

        current_video_ids = set()

        cursor.execute(f"SELECT * FROM staging.{table};")
        rows = cursor.fetchall()

        for row in rows:
            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cursor, conn, schema, transformed_row)
            else:
                transformed_row = transform_data(row)
                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cursor, conn, schema, transformed_row)
                else:
                    insert_rows(cursor, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cursor, conn, schema, ids_to_delete)

        logger.info(f"Staging table {schema}.{table} updated successfully.")

    except Exception as e:
        logger.error(f"Error updating staging table {schema}.{table}: {e}")
        raise e
    
    # Menutup koneksi dan cursor ke database PostgreSQL setelah proses staging data selesai dilakukan
    finally:
        if cursor and conn:
            close_conn_cursor(conn, cursor)