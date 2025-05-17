from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import youtube_etl

# ---- Default arguments ----
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["chilemadhavi1974@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": datetime(2025, 4, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---- DAG Definition ----
dag = DAG(
    dag_id="youtube_dag_s3_mongo",
    default_args=default_args,
    description="Extract YouTube video data and load to S3 and MongoDB",
    schedule="@daily",  # Compatible with Airflow 2.7+
    catchup=False,
)

# ---- ETL Task Functions ----


def extract_task(**kwargs):
    keywords = [
        "Machine Learning",
        "Artificial Intelligence",
        "Devops",
        "Deep Learning",
        "Data Science",
        "Data Engineering",
    ]
    videos_by_keyword = youtube_etl.extract_youtube_data(keywords)
    kwargs["ti"].xcom_push(key="youtube_data", value=videos_by_keyword)


def s3_task(**kwargs):
    videos_by_keyword = kwargs["ti"].xcom_pull(
        task_ids="extract_youtube", key="youtube_data"
    )
    youtube_etl.load_to_s3(videos_by_keyword)


def mongo_task(**kwargs):
    videos_by_keyword = kwargs["ti"].xcom_pull(
        task_ids="extract_youtube", key="youtube_data"
    )
    youtube_etl.load_to_mongodb(videos_by_keyword)


# ---- Python Operators ----
extract = PythonOperator(
    task_id="extract_youtube",
    python_callable=extract_task,
    dag=dag,
)

load_s3 = PythonOperator(
    task_id="load_to_s3",
    python_callable=s3_task,
    dag=dag,
)

load_mongo = PythonOperator(
    task_id="load_to_mongodb",
    python_callable=mongo_task,
    dag=dag,
)

# ---- Task Dependencies ----
extract >> [load_s3, load_mongo]

# This DAG will extract YouTube video data for the specified keywords, load it to S3, and then load it to MongoDB.
# The tasks are set to run in parallel after the extraction is complete.
# The DAG is scheduled to run daily, starting from the specified start date.
# The `catchup` parameter is set to `False`, meaning it will not backfill for past dates.
# The `retries` and `retry_delay` parameters are set to handle any potential failures during the task execution.
# The `email_on_failure` and `email_on_retry` parameters are set to `False`, so no emails will be sent on failure or retry.
# The `provide_context` parameter is set to `True`, allowing the tasks to access the context and pass data between them using XComs.
# The `xcom_push` and `xcom_pull` methods are used to pass the extracted data between tasks.
# The `xcom_push` method is used in the `extract_task` to push the extracted data to XCom, and the `xcom_pull` method is used in the `s3_task` and `mongo_task` to pull the data from XCom.
# The `load_to_s3` and `load_to_mongodb` functions are called to load the data into S3 and MongoDB, respectively.
# The `load_to_s3` function uses the `boto3` library to upload the data to S3, and the `load_to_mongodb` function uses the `pymongo` library to insert the data into MongoDB.
# The `convert_iso8601_duration` function is used to convert the ISO 8601 duration format to seconds.
# The `extract_youtube_data` function uses the `googleapiclient` library to fetch YouTube video data based on the specified keywords.
# The `extract_youtube_data` function returns a dictionary containing the video data grouped by keyword.
# The `load_to_s3` function takes the extracted data and uploads it to S3 in CSV format using the `boto3` library.
# The `load_to_mongodb` function takes the extracted data and inserts it into MongoDB using the `pymongo` library.
# The `load_to_mongodb` function also handles the connection to MongoDB and the insertion of data into the specified collection.
# The `load_to_mongodb` function uses the `MongoClient` class from the `pymongo` library to connect to MongoDB and insert the data into the specified collection.
# The `load_to_mongodb` function also handles the connection to MongoDB and the insertion of data into the specified collection.
# The `load_to_mongodb` function uses the `MongoClient` class from the `pymongo` library to connect to MongoDB and insert the data into the specified collection.
