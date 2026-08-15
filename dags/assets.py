"""Assets shared between DAGs.

Defined here rather than inline so the producing DAG (ingest_transactions)
and the consuming DAG (transform_quality) reference one identity instead of
two string literals that silently drift apart - a typo in either would leave
transform_quality waiting on an asset nothing ever updates.

Not a DAG file; the dag-processor imports it and yields nothing.
"""

from __future__ import annotations

from airflow.sdk import Asset

# URI is an identifier, not a connection string - Airflow never dereferences it.
BRONZE_TRANSACTIONS_RAW = Asset("databricks://fraud.bronze.transactions_raw")
