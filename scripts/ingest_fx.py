"""
Ingest daily FX rates into BigQuery (ext.fx_rates).

- Configurable via config.yaml and optional CLI args
- Retries on HTTP calls
- Idempotent MERGE (upsert) into BigQuery
- Weekend/holiday fill: keeps target calendar day, fills from nearest prior ECB business day
"""

import argparse
import datetime as dt
import logging
from typing import List, Dict, Any

import io
import zipfile

import pandas as pd
import requests
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ----------------------------
# Configuration & CLI parsing
# ----------------------------
def load_config(path: str) -> Dict[str, Any]:
    """Load configuration from a YAML file."""
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError("config.yaml must define a mapping/object")
    return cfg


def resolve_date(s: str | None) -> dt.date:
    """Resolve a date string to a `datetime.date` object."""
    if not s or s.lower() == "yesterday":
        return dt.date.today() - dt.timedelta(days=1)
    return dt.date.fromisoformat(s)


def iter_dates(start: dt.date, end: dt.date):
    """Yield each date from start to end (inclusive)."""
    for n in range((end - start).days + 1):
        yield start + dt.timedelta(days=n)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    ap = argparse.ArgumentParser(description="Ingest FX rates into BigQuery")
    ap.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    ap.add_argument("--start-date", dest="start_date", help='YYYY-MM-DD or "yesterday"')
    ap.add_argument("--end-date", dest="end_date", help='YYYY-MM-DD or "yesterday"')
    return ap.parse_args()


# ----------------------------
# HTTP client with retries
# ----------------------------
def build_http_session() -> requests.Session:
    """Build an HTTP session with retry logic."""
    sess = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    return sess


# ----------------------------
# Fetch & transform
# ----------------------------
def fetch_rates_for_date(
    session: requests.Session,
    ecb_zip_url: str,
    currencies: List[str],
    rate_date: dt.date,
) -> pd.DataFrame:
    """
    Fetch FX rates for a given date, filling from the nearest prior ECB business day.
    """
    r = session.get(ecb_zip_url, timeout=60)
    r.raise_for_status()

    df_all = pd.read_csv(io.BytesIO(r.content), compression="zip")
    df_all["Date"] = pd.to_datetime(df_all["Date"]).dt.date

    # Find nearest prior row (including same day), up to ~2 weeks back
    for offset in range(15):  # Configurable lookback
        target_date = rate_date - dt.timedelta(days=offset)
        row = df_all.loc[df_all["Date"] == target_date]
        if not row.empty:
            break
    else:
        raise RuntimeError(f"No ECB rates found near {rate_date}")

    eur_to_usd = row["USD"].iloc[0]
    if pd.isna(eur_to_usd):
        raise RuntimeError(f"ECB row near {rate_date} missing USD; cannot compute cross-rates")

    records = []
    for cur in set(currencies) | {"USD"}:
        if cur == "USD":
            rate_to_usd = 1.0
        elif cur in row:
            rate_to_usd = eur_to_usd / row[cur].iloc[0]
        else:
            continue
        records.append({"rate_date": rate_date, "currency": cur, "rate_to_usd": rate_to_usd})

    return pd.DataFrame(records)


# ----------------------------
# BigQuery I/O
# ----------------------------
def ensure_table(client: bigquery.Client, table_id: str, schema: List[bigquery.SchemaField]):
    """Ensure the BigQuery table exists."""
    try:
        client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)


def upsert_dataframe(client: bigquery.Client, df: pd.DataFrame, table_id: str):
    """Upsert data into BigQuery."""
    if df.empty:
        logging.info("No rows to upsert")
        return

    temp_table_id = f"{table_id}_tmp"
    job_cfg = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    client.load_table_from_dataframe(df, temp_table_id, job_config=job_cfg).result()

    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S
    ON T.rate_date = S.rate_date AND T.currency = S.currency
    WHEN MATCHED THEN UPDATE SET rate_to_usd = S.rate_to_usd
    WHEN NOT MATCHED THEN INSERT (rate_date, currency, rate_to_usd)
    VALUES (S.rate_date, S.currency, S.rate_to_usd)
    """
    client.query(merge_sql).result()
    client.delete_table(temp_table_id, not_found_ok=True)


# ----------------------------
# Main
# ----------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    cfg = load_config(args.config)

    project_id = cfg["project_id"]
    dataset = cfg.get("dataset", "ext")
    table = cfg.get("table", "fx_rates")
    ecb_zip_url = cfg.get("ecb_zip_url", "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip")
    currencies = cfg.get("currencies", ["EUR", "GBP", "ILS", "CAD", "AUD", "JPY", "CHF"])

    start_date = resolve_date(args.start_date or cfg.get("start_date", "yesterday"))
    end_date = resolve_date(args.end_date or cfg.get("end_date", "yesterday"))

    table_id = f"{project_id}.{dataset}.{table}"
    schema = [
        bigquery.SchemaField("rate_date", "DATE"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("rate_to_usd", "FLOAT"),
    ]

    session = build_http_session()
    bq_client = bigquery.Client(project=project_id)

    ensure_table(bq_client, table_id, schema)

    frames = []
    for d in iter_dates(start_date, end_date):
        df = fetch_rates_for_date(session, ecb_zip_url, currencies, d)
        frames.append(df)

    if frames:
        final_df = pd.concat(frames).drop_duplicates(subset=["rate_date", "currency"])
        upsert_dataframe(bq_client, final_df, table_id)
        logging.info("Upserted %d rows into %s", len(final_df), table_id)
    else:
        logging.info("No data to upsert.")


if __name__ == "__main__":
    main()
