import functions_framework
import requests
import yaml
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import List, Dict
from google.cloud import bigquery

CFG_PATH = Path(__file__).parent / "config.yaml"

def _read_cfg() -> dict:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _parse_date(s: str) -> date:
    if s.lower() == "yesterday":
        return date.today() - timedelta(days=1)
    return datetime.strptime(s, "%Y-%m-%d").date()

def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def _get_param(request, name: str, default: str | None = None) -> str | None:
    """Get value from query string (?name=) or JSON body; fall back to default."""
    val = request.args.get(name) if request else None
    if val is None:
        try:
            body = request.get_json(silent=True) or {}
            val = body.get(name)
        except Exception:
            val = None
    return val if val is not None else default

def _split_csv(s: str) -> List[str]:
    return [t.strip().upper() for t in s.split(",") if t.strip()] if s else []

def _fetch_rates_for_date(base_url: str, iso_date: str, base_ccy: str, symbols: List[str]) -> Dict[str, float]:
    # historical endpoint: https://api.exchangerate.host/{date}?base=USD&symbols=EUR,GBP
    url = f"{base_url.rstrip('/')}/{iso_date}"
    resp = requests.get(url, params={"base": base_ccy, "symbols": ",".join(symbols)}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # exchangerate.host returns {"motd":..., "success": true, "historical": true, "date": "YYYY-MM-DD", "rates": {...}}
    if "rates" not in data:
        raise RuntimeError(f"No 'rates' field in response for {iso_date}: {data}")
    return data["rates"]

def _delete_existing_for_date(bq: bigquery.Client, table_id: str, rate_dt: date):
    q = f"DELETE FROM `{table_id}` WHERE rate_date = @d"
    job = bq.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", rate_dt.isoformat())]
        ),
    )
    job.result()

@functions_framework.http
def ingest_fx(request):
    """
    HTTP Cloud Function.
    - Reads config.yaml for defaults (project_id, dataset, table, base_url, currencies, start/end)
    - Overrides via query/body: ?start=YYYY-MM-DD&end=YYYY-MM-DD&currencies=USD,EUR
    - Loads one row per (rate_date, currency) into BigQuery
    - Idempotent per date: deletes that date first, then inserts fresh rows
    """
    cfg = _read_cfg()

    project_id = cfg["project_id"]
    dataset = cfg["dataset"]
    table = cfg["table"]
    base_url = cfg["base_url"]
    default_currencies = cfg.get("currencies", [])
    default_start = cfg.get("start_date", "yesterday")
    default_end = cfg.get("end_date", "yesterday")

    # Overrides from request (optional)
    start_s = _get_param(request, "start", default_start)
    end_s = _get_param(request, "end", default_end)
    currencies_override = _get_param(request, "currencies", None)

    start_dt = _parse_date(start_s)
    end_dt = _parse_date(end_s)
    if end_dt < start_dt:
        return ({"error": "end < start", "start": start_s, "end": end_s}, 400)

    currencies = _split_csv(currencies_override) if currencies_override else [c.upper() for c in default_currencies]
    if not currencies:
        return ({"error": "No currencies provided"}, 400)

    base_ccy = "USD"  # we keep USD as base to match your dbt logic
    table_id = f"{project_id}.{dataset}.{table}"

    bq = bigquery.Client(project=project_id)

    total_inserted = 0
    results = []

    for d in _daterange(start_dt, end_dt):
        iso = d.isoformat()
        # fetch historical rates for this date
        rates = _fetch_rates_for_date(base_url, iso, base_ccy, currencies)

        # idempotency: delete that date first
        _delete_existing_for_date(bq, table_id, d)

        # insert rows
        rows = []
        for ccy in currencies:
            if ccy not in rates:
                continue
            currency_per_usd = float(rates[ccy])  # e.g., EUR per 1 USD
            if currency_per_usd <= 0:
                continue
            usd_per_currency = 1.0 / currency_per_usd  # USD per 1 EUR
            rows.append({"rate_date": iso, "currency": ccy, "rate_to_usd": usd_per_currency})

        errors = bq.insert_rows_json(table_id, rows)
        if errors:
            return ({"status": "error", "date": iso, "details": errors}, 500)

        total_inserted += len(rows)
        results.append({"date": iso, "inserted": len(rows)})

    return ({"status": "ok", "total_inserted": total_inserted, "by_date": results}, 200)
