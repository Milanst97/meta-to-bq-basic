# Meta Ads to BigQuery

Cloud Run Job that pulls daily ad level insights from the Meta Marketing API and loads them into BigQuery. This is the base template: no client specific logic. Extend it per client with derived fields (location, store type, currency conversion, breakdowns) as needed.

## Tables

Both tables live in `[PROJECT_ID].[DATASET]`, are date partitioned on `date`, and carry `source = "meta"`.

| Table | Content |
| --- | --- |
| `meta_ads_campaign` | Ad level metrics: spend, reach, impressions, clicks |
| `meta_ads_conversions` | One row per action_type per ad per day, count in `value` and money in `action_value` |

## Fetch strategy

Per account and chunk (`CHUNK_DAYS`, default 30):

1. Synchronous insights GET with up to 3 retry attempts (fast path).
2. On failure, async report run: POST, poll until completed, then page results. Handles accounts too heavy for synchronous requests (error subcode 1504044).
3. If async also fails, the chunk is split in half recursively down to single days.

Each account runs inside its own try/except. A failed account is logged as `{account_id} FAILED: ...`, skipped, and listed in the end of run summary. Its existing BigQuery data is untouched. The run only fails hard if every account fails.

## Idempotency

Delete then reinsert per run, scoped by `source`, `account_id IN (pulled accounts)`, and the date window. Accounts removed from `META_AD_ACCOUNT_IDS` keep their historical rows. Reruns of any window are safe.

## Environment variables

| Variable | Default | Notes |
| --- | --- | --- |
| `META_ACCESS_TOKEN` | required | Provide via Secret Manager |
| `META_AD_ACCOUNT_IDS` | required | Comma separated, with or without `act_` prefix |
| `BQ_PROJECT` | required | GCP project ID |
| `BQ_DATASET` | required | BigQuery dataset |
| `META_API_VERSION` | `v25.0` | |
| `ATTRIBUTION_WINDOWS` | `7d_click,1d_view` | The `value` field sums the requested windows |
| `REPORTING_TZ` | `Etc/GMT-1` | Window anchored to yesterday in this TZ |
| `LOOKBACK_DAYS` | `7` | Daily repull window |
| `START_DATE` / `END_DATE` | empty | ISO dates, override lookback for backfills |
| `CHUNK_DAYS` | `30` | Days per API request window |
| `REQUEST_DELAY` | `1` | Seconds between paginated requests |
| `ASYNC_POLL_SECONDS` | `15` | Async job poll interval |
| `ASYNC_TIMEOUT_SECONDS` | `3600` | Max wait per async job |
| `BQ_AD_TABLE` | `meta_ads_campaign` | |
| `BQ_CONVERSIONS_TABLE` | `meta_ads_conversions` | |

## Folder layout

```
[FOLDER_NAME]/
  meta_to_bq.py
  requirements.txt
  Dockerfile
```

`requirements.txt`:

```
requests
google-cloud-bigquery
```

`Dockerfile`:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY meta_to_bq.py .
ENTRYPOINT ["python", "meta_to_bq.py"]
```

## Deploy

The service account needs Secret Manager Secret Accessor on the token secret and BigQuery Data Editor plus Job User on the target dataset.

```
cd [FOLDER_NAME]

gcloud run jobs deploy [JOB_NAME] \
  --source . \
  --region [REGION] \
  --service-account [SERVICE_ACCOUNT] \
  --set-secrets "META_ACCESS_TOKEN=[SECRET_NAME]:latest" \
  --set-env-vars "^##^META_AD_ACCOUNT_IDS=[IDS]##BQ_PROJECT=[PROJECT_ID]##BQ_DATASET=[DATASET]" \
  --task-timeout 3600 \
  --memory 2Gi \
  --max-retries 0

gcloud run jobs execute [JOB_NAME] --region [REGION]
```

## Backfill

Run month by month with `--wait` so executions do not overlap:

```
gcloud run jobs execute [JOB_NAME] --region [REGION] --wait --update-env-vars "START_DATE=2024-01-01,END_DATE=2024-01-31"
```

After the last month, clear the override so scheduled runs return to the rolling window:

```
gcloud run jobs update [JOB_NAME] --region [REGION] --remove-env-vars START_DATE,END_DATE
```

## Operational notes

- Permission errors (code 200) mean the ad account is not assigned to the token's system user in Business Manager with at least ads_read.
- `sync ... failed, going async` log lines identify accounts too heavy for the fast path.
- If the token is shared across multiple jobs, app level rate limits are shared. Offset schedules if code 4 or 17 retries appear in logs.
- To pull per attribution window splits, each action in the API response carries per window keys (`7d_click`, `1d_view`) alongside the summed `value`.
