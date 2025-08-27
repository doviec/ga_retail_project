"""
Ingest daily FX rates into BigQuery (ext.fx_rates).

- Configurable via config.yaml and optional CLI args
- Retries on HTTP calls
- Idempotent MERGE (upsert) into BigQuery
- Clean naming (snake_case), typing, logging
"""

from __future__ import annotations
import argparse
import datetime as dt
import logging
from typing import List, Dict, Any

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
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError("config.yaml must define a mapping/object")
    return cfg


def resolve_date(s: str | None) -> dt.date:
    if not s or s.lower() == "yesterday":
        return dt.date.today() - dt.timedelta(days=1)
    return dt.date.fromisoformat(s)


def iter_dates(start: dt.date, end: dt.date):
    """
    Yield each date from start to end (inclusive).
    """
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Ingest FX rates into BigQuery")
    ap.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    ap.add_argument("--start-date", dest="start_date", help='YYYY-MM-DD or "yesterday"')
    ap.add_argument("--end-date", dest="end_date", help='YYYY-MM-DD or "yesterday"')
    return ap.parse_args()


# ----------------------------
# HTTP client with retries
# ----------------------------
def build_http_session() -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=3, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    return sess


# ----------------------------
# Fetch & transform
# ----------------------------
def fetch_rates_for_date(
    session: requests.Session,
    base_url: str,
    currencies: List[str],
    rate_date: dt.date
) -> pd.DataFrame:
    """
    Fetch EUR-based daily FX rates from the ECB historical ZIP (no API key).
    Then compute rate_to_usd = 1 CUR in USD via cross-rate:
      rate_to_usd = (EUR->USD) / (EUR->CUR)
    Special cases:
      - If CUR == USD => 1.0
      - If EUR->CUR missing => skip that currency
    """
    # ECB historical ZIP (CSV) – no API key
    ecb_zip_url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"

    # Download zip to memory and read the CSV inside
    r = session.get(ecb_zip_url, timeout=60)
    r.raise_for_status()

    # pandas can read a CSV inside a zip via BytesIO + compression='zip'
    import io, zipfile
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    # The file is usually named 'eurofxref-hist.csv'
    csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
    with zf.open(csv_name) as f:
        df_all = pd.read_csv(f)

    # Normalize column names: 'Date','USD','GBP','ILS', ...
    df_all["Date"] = pd.to_datetime(df_all["Date"]).dt.date
    row = df_all.loc[df_all["Date"] == rate_date]

    if row.empty:
        # No rate for weekends/holidays – fallback to previous business day
        prev = rate_date - dt.timedelta(days=1)
        while prev >= rate_date - dt.timedelta(days=7):
            row = df_all.loc[df_all["Date"] == prev]
            if not row.empty:
                rate_date = prev
                break
            prev -= dt.timedelta(days=1)

    if row.empty:
        raise RuntimeError(f"No ECB rates found around {rate_date}")

    row = row.iloc[0]  # series with columns: Date, USD, GBP, ...

    # Ensure USD is present for cross-rate
    if pd.isna(row.get("USD")):
        raise RuntimeError(f"ECB row for {rate_date} missing USD; cannot compute cross-rates")

    eur_to_usd = float(row["USD"])

    # Always include USD so we can return 1.0 for USD
    cur_set = set(currencies)
    cur_set.add("USD")

    records = []
    for cur in sorted(cur_set):
        if cur == "USD":
            rate_to_usd = 1.0
        else:
            eur_to_cur = row.get(cur)
            if pd.isna(eur_to_cur):
                # Skip currencies not provided by ECB for that day
                continue
            eur_to_cur = float(eur_to_cur)
            if eur_to_cur == 0:
                continue
            # 1 CUR = (EUR->USD) / (EUR->CUR) USD
            rate_to_usd = eur_to_usd / eur_to_cur
        records.append({"rate_date": rate_date, "currency": cur, "rate_to_usd": rate_to_usd})

    df = pd.DataFrame.from_records(records, columns=["rate_date", "currency", "rate_to_usd"])
    if df.empty:
        raise RuntimeError(f"Empty DF after ECB processing for {rate_date}")
    return df


# ----------------------------
# BigQuery I/O
# ----------------------------
def ensure_table(client: bigquery.Client, table_id: str):
    """
    Create target table if it doesn't exist.
    Schema: rate_date DATE, currency STRING, rate_to_usd FLOAT64
    """
    try:
        client.get_table(table_id)
        return
    except NotFound:
        pass

    schema = [
        bigquery.SchemaField("rate_date", "DATE"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("rate_to_usd", "FLOAT"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)


def upsert_dataframe(client: bigquery.Client, df: pd.DataFrame, table_id: str):
    """
    Idempotent upsert: load to temp, then MERGE into target on (rate_date, currency).
    """
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
    WHEN MATCHED THEN
      UPDATE SET rate_to_usd = S.rate_to_usd
    WHEN NOT MATCHED THEN
      INSERT (rate_date, currency, rate_to_usd)
      VALUES (S.rate_date, S.currency, S.rate_to_usd)
    """
    client.query(merge_sql).result()
    client.delete_table(temp_table_id, not_found_ok=True)


def validate_config(cfg: Dict[str, Any]):
    required_keys = ["project_id"]
    for key in required_keys:
        if key not in cfg:
            raise ValueError(f"Missing required config key: {key}")


# ----------------------------
# Main
# ----------------------------
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        args = parse_args()
        cfg = load_config(args.config)
        validate_config(cfg)

        project_id: str = cfg["project_id"]
        dataset: str = cfg.get("dataset", "ext")
        table: str = cfg.get("table", "fx_rates")
        base_url: str = cfg.get("base_url", "https://api.exchangerate.host")
        currencies: List[str] = cfg.get("currencies", ["USD", "EUR", "GBP", "ILS", "CAD", "AUD"])

        # Allow CLI to override dates, else use config defaults
        start_str = args.start_date or cfg.get("start_date", "yesterday")
        end_str = args.end_date or cfg.get("end_date", "yesterday")
        start_date = resolve_date(start_str)
        end_date = resolve_date(end_str)

        table_id = f"{project_id}.{dataset}.{table}"

        logging.info("Project: %s | Table: %s", project_id, table_id)
        logging.info("Date range: %s -> %s | Currencies: %s", start_date, end_date, ",".join(currencies))

        session = build_http_session()
        bq_client = bigquery.Client(project=project_id)

        ensure_table(bq_client, table_id)

        all_frames: List[pd.DataFrame] = []
        for d in iter_dates(start_date, end_date):
            df = fetch_rates_for_date(session, base_url, currencies, d)
            logging.info("Fetched %d rows for %s", len(df), d)
            all_frames.append(df)

        if all_frames:
            final_df = pd.concat(all_frames, ignore_index=True)
            upsert_dataframe(bq_client, final_df, table_id)
            logging.info("Upserted %d rows into %s", len(final_df), table_id)
        else:
            logging.info("No data frames collected; nothing to upsert.")

        logging.info("Done.")
    except Exception as e:
        logging.exception("Fatal error: %s", e)
        exit(1)


if __name__ == "__main__":
    main()
