"""Bridge Kafka `transactions` -> Volume -> Bronze, every 15 minutes.

consume_batch bounds the read to [data_interval_start, data_interval_end)
via offsets_for_times; upload_to_volume lands the Parquet in the UC volume;
run_copy_into loads it into fraud.bronze.transactions_raw.

Retry-safe: bounded read always resolves the same offsets, upload overwrites
the same deterministic filename, and COPY INTO dedupes by file path.

Caveat: COPY INTO tracks file path, not content, so in-window messages
produced after a retry's upload aren't picked up (no grace-period watermark
yet).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import CronDataIntervalTimetable
from confluent_kafka import Consumer, TopicPartition

from assets import BRONZE_TRANSACTIONS_RAW
from producer import PARQUET_SCHEMA, TOPIC

KAFKA_BOOTSTRAP = "kafka:19092"  # internal listener; producer.py's localhost:9092 is host-only
STAGING_DIR = Path("/opt/airflow/staging")
VOLUME_DIR = "/Volumes/fraud/bronze/landing"
POLL_IDLE_TIMEOUT_SECONDS = 30  # guards against hanging if Kafka stalls mid-read

# Scans the whole landing dir every run; COPY INTO's own file-tracking
# handles idempotency.
COPY_INTO_SQL = f"""
COPY INTO fraud.bronze.transactions_raw
FROM (
    SELECT
        *,
        current_timestamp() AS _ingested_at,
        _metadata.file_path AS _source_file
    FROM '{VOLUME_DIR}/'
)
FILEFORMAT = PARQUET
"""


def _bounded_offsets(consumer: Consumer, partitions: list[int], ts: datetime) -> dict[int, int]:
    """Offset of the first message at/after ts, per partition. Falls back to
    the high watermark when offsets_for_times finds none (ts is beyond every
    message produced so far)."""
    ts_ms = int(ts.timestamp() * 1000)
    queried = consumer.offsets_for_times(
        [TopicPartition(TOPIC, p, ts_ms) for p in partitions], timeout=10.0
    )
    bounds = {}
    for tp in queried:
        if tp.offset >= 0:
            bounds[tp.partition] = tp.offset
        else:
            _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, tp.partition), timeout=10.0)
            bounds[tp.partition] = high
    return bounds


@dag(
    dag_id="ingest_transactions",
    # plain cron would default to CronTriggerTimetable (start == end, no window)
    schedule=CronDataIntervalTimetable("*/15 * * * *", timezone="UTC"),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"retries": 0},
    tags=["ingest", "bronze"],
    doc_md=__doc__,
)
def ingest_transactions():

    @task
    def consume_batch(data_interval_start=None, data_interval_end=None) -> str:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "ingest_transactions",  # unused for offset commit; partitions are assign()ed explicitly
            "enable.auto.commit": False,
        })
        try:
            partitions = list(consumer.list_topics(TOPIC, timeout=10.0).topics[TOPIC].partitions.keys())
            start_offsets = _bounded_offsets(consumer, partitions, data_interval_start)
            end_offsets = _bounded_offsets(consumer, partitions, data_interval_end)

            remaining = {p: max(0, end_offsets[p] - start_offsets[p]) for p in partitions}
            consumer.assign([
                TopicPartition(TOPIC, p, start_offsets[p]) for p in partitions if remaining[p] > 0
            ])

            events = []
            idle_since = datetime.now(timezone.utc)
            while any(v > 0 for v in remaining.values()):
                msg = consumer.poll(1.0)
                if msg is None:
                    if (datetime.now(timezone.utc) - idle_since).total_seconds() > POLL_IDLE_TIMEOUT_SECONDS:
                        break
                    continue
                idle_since = datetime.now(timezone.utc)
                if msg.error():
                    raise RuntimeError(f"Kafka consumer error: {msg.error()}")
                p = msg.partition()
                if msg.offset() >= end_offsets[p]:
                    remaining[p] = 0
                    continue
                events.append(json.loads(msg.value()))
                remaining[p] -= 1
        finally:
            consumer.close()

        if not events:
            raise AirflowSkipException(
                f"No messages in [{data_interval_start.isoformat()}, {data_interval_end.isoformat()})"
            )

        events.sort(key=lambda e: e["event_time"])
        table = pa.Table.from_pylist(events, schema=PARQUET_SCHEMA)
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        path = STAGING_DIR / f"transactions_{data_interval_start:%Y%m%dT%H%M%S}.parquet"
        pq.write_table(table, path)
        return str(path)

    @task(retries=2, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True)
    def upload_to_volume(local_path: str) -> str:
        from databricks.sdk import WorkspaceClient

        volume_path = f"{VOLUME_DIR}/{Path(local_path).name}"
        with open(local_path, "rb") as f:
            WorkspaceClient().files.upload(file_path=volume_path, contents=f, overwrite=True)
        return volume_path

    # outlet on this task, not the DAG - an empty-window skip upstream means
    # this never runs, so no pointless dbt build fires downstream.
    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        outlets=[BRONZE_TRANSACTIONS_RAW],
    )
    def run_copy_into(volume_path: str) -> None:
        from databricks import sql as databricks_sql

        with databricks_sql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(COPY_INTO_SQL)

    run_copy_into(upload_to_volume(consume_batch()))


ingest_transactions()
