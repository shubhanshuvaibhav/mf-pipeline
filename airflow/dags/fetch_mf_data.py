from datetime import datetime, timedelta
import json
import logging
import os
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DBT_DIR = "/opt/airflow/dbt_project"
logger = logging.getLogger(__name__)


def parse_scheme_codes(value):
    scheme_codes = [code.strip() for code in value.split(",") if code.strip()]
    if not scheme_codes:
        raise ValueError("SCHEME_CODES must contain at least one scheme code")
    return list(dict.fromkeys(scheme_codes))


def validate_payload(payload):
    if payload.get("status") != "SUCCESS":
        raise ValueError(f"API returned status {payload.get('status')!r}")
    if not isinstance(payload.get("data"), list):
        raise ValueError("API response data must be a list")
    return payload


@task
def get_scheme_codes():
    return parse_scheme_codes(os.environ.get("SCHEME_CODES", ""))


def create_api_session():
    retry_policy = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_policy))
    return session


@task
def fetch_scheme_nav(scheme_code):
    context = get_current_context()
    logical_date = context["logical_date"]
    run_id = context["run_id"]
    output_dir = f"/opt/airflow/data/bronze/{logical_date:%Y-%m-%d}"
    os.makedirs(output_dir, exist_ok=True)
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    response = None

    try:
        with create_api_session() as session:
            url = f"https://api.mfapi.in/mf/{scheme_code}"
            response = session.get(url, timeout=(10, 30))
            response.raise_for_status()
            payload = response.json()

        validate_payload(payload)

        nav_record_count = len(payload["data"])
        latest_nav_date = max(
            (
                datetime.strptime(item["date"], "%d-%m-%Y").date()
                for item in payload["data"]
            ),
            default=None,
        )
        response_bytes = len(response.content)
        payload_json = json.dumps(payload, sort_keys=True)
        snapshot_hash = pg_hook.get_first(
            """
            SELECT encode(digest(%s::jsonb::text, 'sha256'), 'hex')
            """,
            parameters=(payload_json,),
        )[0]

        file_path = os.path.join(
            output_dir, f"raw_nav_data_{scheme_code}.json"
        )
        with open(file_path, "w") as file:
            json.dump(payload, file)

        insert_result = pg_hook.get_first(
            """
            INSERT INTO bronze.raw_dump
                (run_id, scheme_code, snapshot_hash, raw_payload)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (scheme_code, snapshot_hash) DO NOTHING
            RETURNING raw_dump_id
            """,
            parameters=(run_id, scheme_code, snapshot_hash, payload_json),
        )
        inserted = insert_result is not None
        pg_hook.run(
            """
            INSERT INTO control.ingestion_audit
                (run_id, scheme_code, status, nav_record_count,
                 snapshot_hash, inserted, http_status, response_bytes,
                 latest_nav_date, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            parameters=(
                run_id,
                scheme_code,
                "success",
                nav_record_count,
                snapshot_hash,
                inserted,
                response.status_code,
                response_bytes,
                latest_nav_date,
            ),
        )
        logger.info(
            "Ingested scheme_code=%s, nav_records=%s, inserted=%s, "
            "latest_nav_date=%s, response_bytes=%s, backup_file=%s",
            scheme_code,
            nav_record_count,
            inserted,
            latest_nav_date,
            response_bytes,
            file_path,
        )
        return {
            "scheme_code": scheme_code,
            "status": "success",
            "nav_record_count": nav_record_count,
            "latest_nav_date": str(latest_nav_date),
            "inserted": inserted,
        }
    except Exception as error:
        pg_hook.run(
            """
            INSERT INTO control.ingestion_audit
                (run_id, scheme_code, status, http_status, response_bytes,
                 completed_at, error_message)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
            """,
            parameters=(
                run_id,
                scheme_code,
                "failed",
                response.status_code if response is not None else None,
                len(response.content) if response is not None else None,
                str(error),
            ),
        )
        logger.exception("Failed to ingest scheme_code=%s", scheme_code)
        raise


def print_pipeline_summary(**kwargs):
    run_id = kwargs["run_id"]
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    audit_rows = pg_hook.get_records(
        """
        SELECT scheme_code, status, nav_record_count, http_status,
               response_bytes, latest_nav_date, inserted
        FROM (
            SELECT
                scheme_code,
                status,
                nav_record_count,
                http_status,
                response_bytes,
                latest_nav_date,
                inserted,
                ROW_NUMBER() OVER(
                    PARTITION BY scheme_code
                    ORDER BY ingestion_id DESC
                ) AS record_rank
            FROM control.ingestion_audit
            WHERE run_id = %s
        ) AS latest_audit
        WHERE record_rank = 1
        ORDER BY scheme_code
        """,
        parameters=(run_id,),
    )
    staging_rows = pg_hook.get_first(
        "SELECT COUNT(*) FROM analytics_staging.stg_mf_nav"
    )[0]
    mart_rows = pg_hook.get_first(
        "SELECT COUNT(*) FROM analytics_marts.fct_fund_performance"
    )[0]
    successful_rows = [row for row in audit_rows if row[1] == "success"]
    failed_rows = [row for row in audit_rows if row[1] == "failed"]
    total_nav_records = sum(row[2] or 0 for row in successful_rows)
    inserted_snapshots = sum(row[6] is True for row in successful_rows)
    duplicate_snapshots = sum(row[6] is False for row in successful_rows)

    logger.info("Pipeline completed successfully")
    logger.info("Run ID: %s", run_id)
    logger.info("Schemes processed: %s", len(audit_rows))
    logger.info("Schemes succeeded: %s", len(successful_rows))
    logger.info("Schemes failed: %s", len(failed_rows))
    logger.info("NAV records received: %s", total_nav_records)
    logger.info("New snapshots inserted: %s", inserted_snapshots)
    logger.info("Duplicate snapshots skipped: %s", duplicate_snapshots)
    logger.info("Staging rows: %s", staging_rows)
    logger.info("Mart rows: %s", mart_rows)
    logger.info(
        "Latest NAV dates: %s",
        [(row[0], row[5]) for row in successful_rows],
    )
    logger.info("Per-scheme results: %s", audit_rows)


default_args = {
    "owner": "engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "mf_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    scheme_codes = get_scheme_codes()
    ingest_tasks = fetch_scheme_nav.expand(scheme_code=scheme_codes)

    dbt_run = BashOperator(
        task_id="dbt_run_transformations",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_validations",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    pipeline_summary = PythonOperator(
        task_id="print_pipeline_summary",
        python_callable=print_pipeline_summary,
    )

    ingest_tasks >> dbt_run >> dbt_test >> pipeline_summary