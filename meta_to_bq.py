import os
import json
import time
import random
import datetime as dt
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery


META_ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]
META_AD_ACCOUNT_IDS = os.environ["META_AD_ACCOUNT_IDS"]
META_API_VERSION = os.getenv("META_API_VERSION", "v25.0")

ATTRIBUTION_WINDOWS = os.getenv("ATTRIBUTION_WINDOWS", "7d_click,1d_view")
REPORTING_TZ = os.getenv("REPORTING_TZ", "Etc/GMT-1")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

START_DATE = os.getenv("START_DATE", "").strip()
END_DATE = os.getenv("END_DATE", "").strip()

CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "30"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1"))

ASYNC_POLL_SECONDS = int(os.getenv("ASYNC_POLL_SECONDS", "15"))
ASYNC_TIMEOUT_SECONDS = int(os.getenv("ASYNC_TIMEOUT_SECONDS", "3600"))

BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
AD_TABLE = os.getenv("BQ_AD_TABLE", "meta_ads_campaign")
CONVERSIONS_TABLE = os.getenv("BQ_CONVERSIONS_TABLE", "meta_ads_conversions")

SOURCE = "meta"


def parse_account_ids(v):
    ids = []
    for raw in (v or "").split(","):
        a = raw.strip()
        if not a:
            continue
        ids.append(a[4:] if a.startswith("act_") else a)
    return ids


def safe_int(x):
    try:
        return int(float(x))
    except Exception:
        return None


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def safe_str(x):
    if x is None:
        return None
    return str(x)


def get_date_range():
    if START_DATE and END_DATE:
        return dt.date.fromisoformat(START_DATE), dt.date.fromisoformat(END_DATE)
    tz = ZoneInfo(REPORTING_TZ)
    today = dt.datetime.now(tz).date()
    until = today - dt.timedelta(days=1)
    since = until - dt.timedelta(days=LOOKBACK_DAYS - 1)
    return since, until


def parse_attribution_windows(v):
    return json.dumps([w.strip() for w in (v or "").split(",") if w.strip()])


RATE_LIMIT_CODES = {4, 17, 32, 613}
TRANSIENT_CODES = {1, 2} | RATE_LIMIT_CODES


def request_with_retries(url, params, headers, max_attempts=10):
    last_r = None
    last_body = None

    for attempt in range(1, max_attempts + 1):
        r = requests.get(url, params=params, headers=headers, timeout=90)
        last_r = r

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(300, 30 * attempt) + random.random())
            continue

        try:
            body = r.json()
        except Exception:
            body = {"non_json": r.text[:2000]}
        last_body = body

        if not r.ok:
            err = body.get("error", {})
            code = err.get("code")
            transient = err.get("is_transient") or code in TRANSIENT_CODES
            if transient and attempt < max_attempts:
                if code in RATE_LIMIT_CODES:
                    time.sleep(min(600, 60 * attempt) + random.random())
                else:
                    time.sleep(min(60, 2 ** attempt) + random.random())
                continue
            return r, body

        return r, body

    return last_r, (last_body or {"non_json": getattr(last_r, "text", "")[:2000]})


def post_with_retries(url, data, headers, max_attempts=10):
    last_r = None
    last_body = None

    for attempt in range(1, max_attempts + 1):
        r = requests.post(url, data=data, headers=headers, timeout=90)
        last_r = r

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(300, 30 * attempt) + random.random())
            continue

        try:
            body = r.json()
        except Exception:
            body = {"non_json": r.text[:2000]}
        last_body = body

        if not r.ok:
            err = body.get("error", {})
            code = err.get("code")
            transient = err.get("is_transient") or code in RATE_LIMIT_CODES
            if transient and attempt < max_attempts:
                time.sleep(min(600, 60 * attempt) + random.random())
                continue
            return r, body

        return r, body

    return last_r, (last_body or {"non_json": getattr(last_r, "text", "")[:2000]})


def start_report_run(account_id, params, headers):
    url = f"https://graph.facebook.com/{META_API_VERSION}/act_{account_id}/insights"
    r, body = post_with_retries(url, params, headers)
    if not r.ok:
        raise RuntimeError(json.dumps(body))
    report_run_id = body.get("report_run_id")
    if not report_run_id:
        raise RuntimeError(json.dumps(body))
    return report_run_id


def wait_for_report(report_run_id, headers):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{report_run_id}"
    deadline = time.time() + ASYNC_TIMEOUT_SECONDS
    while time.time() < deadline:
        r, body = request_with_retries(url, None, headers)
        if not r.ok:
            raise RuntimeError(json.dumps(body))
        status = body.get("async_status")
        if status == "Job Completed":
            return
        if status in ("Job Failed", "Job Skipped"):
            raise RuntimeError(f"async job {report_run_id} status={status}")
        time.sleep(ASYNC_POLL_SECONDS)
    raise RuntimeError(f"async job {report_run_id} timed out")


def fetch_report_results(report_run_id, headers):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{report_run_id}/insights"
    params = {"limit": 500}
    rows = []
    while True:
        r, body = request_with_retries(url, params, headers)
        if not r.ok:
            raise RuntimeError(json.dumps(body))
        rows.extend(body.get("data", []))
        next_url = body.get("paging", {}).get("next")
        if not next_url:
            return rows
        url = next_url
        params = None
        time.sleep(REQUEST_DELAY)


def fetch_chunk_sync(account_id, params, headers):
    url = f"https://graph.facebook.com/{META_API_VERSION}/act_{account_id}/insights"
    params = dict(params)
    params["limit"] = 1000
    rows = []
    while True:
        r, body = request_with_retries(url, params, headers, max_attempts=3)
        if not r.ok:
            raise RuntimeError(json.dumps(body))
        rows.extend(body.get("data", []))
        next_url = body.get("paging", {}).get("next")
        if not next_url:
            return rows
        url = next_url
        params = None
        time.sleep(REQUEST_DELAY)


def fetch_chunk(account_id, chunk_start, chunk_end, fields, aw, headers):
    params = {
        "level": "ad",
        "time_increment": 1,
        "time_range": json.dumps({"since": chunk_start.isoformat(), "until": chunk_end.isoformat()}),
        "fields": fields,
        "action_attribution_windows": aw,
    }

    try:
        return fetch_chunk_sync(account_id, params, headers)
    except Exception as e:
        print(f"{account_id} sync {chunk_start.isoformat()}..{chunk_end.isoformat()} failed ({e}), going async", flush=True)

    try:
        report_run_id = start_report_run(account_id, params, headers)
        wait_for_report(report_run_id, headers)
        return fetch_report_results(report_run_id, headers)
    except Exception as e:
        days = (chunk_end - chunk_start).days + 1
        if days <= 1:
            raise
        mid = chunk_start + dt.timedelta(days=days // 2 - 1)
        print(f"{account_id} chunk {chunk_start.isoformat()}..{chunk_end.isoformat()} failed ({e}), splitting", flush=True)
        left = fetch_chunk(account_id, chunk_start, mid, fields, aw, headers)
        right = fetch_chunk(account_id, mid + dt.timedelta(days=1), chunk_end, fields, aw, headers)
        return left + right


def meta_fetch_insights(account_id, since, until):
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}"}
    fields = ",".join([
        "date_start", "date_stop",
        "account_id",
        "campaign_id", "campaign_name",
        "adset_id", "adset_name",
        "ad_id", "ad_name",
        "spend", "reach", "impressions", "clicks",
        "actions", "action_values",
    ])
    aw = parse_attribution_windows(ATTRIBUTION_WINDOWS)

    all_rows = []
    chunk_start = since
    while chunk_start <= until:
        chunk_end = min(chunk_start + dt.timedelta(days=CHUNK_DAYS - 1), until)

        data = fetch_chunk(account_id, chunk_start, chunk_end, fields, aw, headers)
        all_rows.extend(data)

        print(f"{account_id} {chunk_start.isoformat()}..{chunk_end.isoformat()}: {len(data)} rows", flush=True)

        chunk_start = chunk_end + dt.timedelta(days=1)
        time.sleep(REQUEST_DELAY)

    return all_rows


def build_ad_rows(raw, extracted_at):
    rows = []
    for r in raw:
        rows.append({
            "date": r.get("date_start"),
            "date_stop": r.get("date_stop"),
            "account_id": safe_str(r.get("account_id")),
            "campaign_id": safe_str(r.get("campaign_id")),
            "campaign_name": r.get("campaign_name"),
            "adset_id": safe_str(r.get("adset_id")),
            "adset_name": r.get("adset_name"),
            "ad_id": safe_str(r.get("ad_id")),
            "ad_name": r.get("ad_name"),
            "source": SOURCE,
            "spend": safe_float(r.get("spend")),
            "reach": safe_int(r.get("reach")),
            "impressions": safe_int(r.get("impressions")),
            "clicks": safe_int(r.get("clicks")),
            "extracted_at": extracted_at,
        })
    return rows


def build_conversion_rows(raw, extracted_at):
    rows = []
    for r in raw:
        actions = r.get("actions") or []
        if not actions:
            continue
        values = {}
        for v in (r.get("action_values") or []):
            at = v.get("action_type")
            if at:
                values[at] = safe_float(v.get("value"))
        for a in actions:
            action_type = a.get("action_type")
            rows.append({
                "date": r.get("date_start"),
                "account_id": safe_str(r.get("account_id")),
                "campaign_id": safe_str(r.get("campaign_id")),
                "adset_id": safe_str(r.get("adset_id")),
                "ad_id": safe_str(r.get("ad_id")),
                "action_type": action_type,
                "value": safe_int(a.get("value")),
                "action_value": values.get(action_type),
                "source": SOURCE,
                "extracted_at": extracted_at,
            })
    return rows


def ad_schema():
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("date_stop", "DATE"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("adset_id", "STRING"),
        bigquery.SchemaField("adset_name", "STRING"),
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("ad_name", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("spend", "FLOAT"),
        bigquery.SchemaField("reach", "INTEGER"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ]


def conversions_schema():
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("adset_id", "STRING"),
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("action_type", "STRING"),
        bigquery.SchemaField("value", "INTEGER"),
        bigquery.SchemaField("action_value", "FLOAT"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ]


def ensure_table(client, table_id, schema):
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    client.create_table(table, exists_ok=True)


def delete_window(client, table_id, account_ids, since, until):
    query = f"""
    DELETE FROM `{table_id}`
    WHERE source = @source
      AND account_id IN UNNEST(@accounts)
      AND `date` BETWEEN @since AND @until
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source", "STRING", SOURCE),
            bigquery.ArrayQueryParameter("accounts", "STRING", account_ids),
            bigquery.ScalarQueryParameter("since", "DATE", since),
            bigquery.ScalarQueryParameter("until", "DATE", until),
        ]
    )
    client.query(query, job_config=job_config).result()


def insert_rows(client, table_id, rows, schema):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
    )
    client.load_table_from_json(rows, table_id, job_config=job_config).result()


def write_window(client, table_id, schema, rows, account_ids, since, until):
    ensure_table(client, table_id, schema)
    delete_window(client, table_id, account_ids, since, until)
    if rows:
        insert_rows(client, table_id, rows, schema)


def main():
    account_ids = parse_account_ids(META_AD_ACCOUNT_IDS)
    if not account_ids:
        return

    since, until = get_date_range()
    extracted_at = dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"window {since.isoformat()}..{until.isoformat()} accounts={account_ids}", flush=True)

    ad_rows = []
    conversion_rows = []
    pulled_accounts = []
    errors = {}

    for account_id in account_ids:
        try:
            raw = meta_fetch_insights(account_id, since, until)
            account_ad_rows = build_ad_rows(raw, extracted_at)
            account_conversion_rows = build_conversion_rows(raw, extracted_at)
        except Exception as e:
            errors[account_id] = str(e)
            print(f"{account_id} FAILED: {e}", flush=True)
            continue

        ad_rows.extend(account_ad_rows)
        conversion_rows.extend(account_conversion_rows)
        pulled_accounts.append(account_id)
        print(f"{account_id} done", flush=True)

    if not pulled_accounts:
        raise RuntimeError(f"all accounts failed: {json.dumps(errors)}")

    client = bigquery.Client(project=BQ_PROJECT)
    base = f"{BQ_PROJECT}.{BQ_DATASET}"

    write_window(client, f"{base}.{AD_TABLE}", ad_schema(), ad_rows, pulled_accounts, since, until)
    write_window(client, f"{base}.{CONVERSIONS_TABLE}", conversions_schema(), conversion_rows, pulled_accounts, since, until)

    print(f"finished accounts={len(pulled_accounts)} failed={len(errors)} errors={json.dumps(errors) if errors else 'none'}", flush=True)


if __name__ == "__main__":
    main()
