import os
import pytest
import psycopg2
from unittest import mock #digunakan untuk membuat mock object yang dapat digunakan untuk menguji kode tanpa benar-benar menjalankan kode tersebut
from airflow.models import Variable, Connection, DagBag #digunakan untuk mengelola variabel dan koneksi di Airflow, serta untuk memuat DAGs dari file

# Step ini untuk memvalidasi API key yang digunakan dengan mock
@pytest.fixture #digunakan untuk membuat fixture yang dapat digunakan di beberapa test case
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")

#Step ini untuk memvalidasi channel handle yang digunakan dengan mock
@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MOCK_CHANNEL_HANDLE"):
        yield Variable.get("CHANNEL_HANDLE")

# Step ini untuk memvalidasi koneksi ke database PostgreSQL dengan mock
@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name",
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")

# Step ini untuk memvalidasi DAGs yang digunakan
@pytest.fixture()
def dagbag():
    yield DagBag()

# Step ini untuk memvalidasi variabel Airflow
@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)

    return get_airflow_variable

# Step ini untuk memvalidasi koneksi ke database PostgreSQL yang sebenarnya
@pytest.fixture
def real_postgres_connection():
    host=os.getenv("POSTGRES_CONN_HOST")
    port=os.getenv("POSTGRES_CONN_PORT")
    user=os.getenv("ELT_DATABASE_USERNAME")
    password=os.getenv("ELT_DATABASE_PASSWORD")
    dbname=os.getenv("ELT_DATABASE_NAME")

    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )
        yield conn

    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()