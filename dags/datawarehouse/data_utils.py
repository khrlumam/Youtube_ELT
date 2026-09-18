# Fungsi untuk menghubungkan ke database PostgreSQL menggunakan Airflow's PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Fungsi untuk berintraksi dengan database PostgreSQL menggunakan Airflow's PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

# Fungsi untuk mendapatkan koneksi dan cursor ke database PostgreSQL
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor

# Fungsi untuk menutup koneksi dan cursor ke database PostgreSQL
def close_conn_cursor(conn, cursor):
    cursor.close()
    conn.close()

# Fungsi untuk membuat schema di database PostgreSQL
def create_schema(schema):
    conn, cursor = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}" 
    cursor.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)

# Fungsi untuk membuat tabel di database PostgreSQL sesuai dengan schema yang ditentukan
def create_table(schema):
    conn, cursor = get_conn_cursor()
    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    else:
        table_sql = f"""
                  CREATE TABLE IF NOT EXISTS {schema}.{table} (
                      "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL,
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
              """

    cursor.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn, cursor)

# Fungsi untuk mendapatkan daftar video_id dari tabel di database PostgreSQL sesuai dengan schema yang ditentukan
def get_video_ids(cursor, schema):

    # Mengambil semua video_id dari tabel di database PostgreSQL sesuai dengan schema yang ditentukan
    cursor.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cursor.fetchall()

    # Mengambil video_id dari hasil query dan menyimpannya dalam list
    video_ids = [row["Video_ID"] for row in ids]

    return video_ids

