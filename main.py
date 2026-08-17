"""Extract item balance activity + cost from the Rista inventory API and land
date-partitioned Parquet files in Azure Blob Storage.

Blob is the landing zone for the Snowflake RAW layer: an external stage /
Snowpipe copies each partition into RAW and stamps the ingestion metadata
columns that the dbt bronze model reads. See snowflake/raw_ingestion.sql.

All configuration is read from environment variables. See .env.example.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import jwt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==============================
# Column mapping and schema
# ==============================

COLUMN_MAPPING = {
    "Date": "Date",
    "StoreName": "Store_Name",
    "StoreCode": "Store_Code",
    "StoreLabel": "Branch_Labels",
    "skuCode": "SKU",
    "itemType": "TYPE",
    "categoryName": "Category",
    "itemName": "NAME",
    "measuringUnit": "MEASURING_UNIT",
    "openingBalance": "Opening_Balance",
    "openingCost": "Opening_Cost",
    "closingBalance": "Closing_Balance",
    "closingCost": "Closing_Cost",
    "activity": "Activity",
    "Yield": "Yield",
    "activities_Sales_qty": "Sales",
    "activities_Sales_cost": "Sales_Cost",
    "Returns": "Returns",
    "Returns_Cost": "Returns_Cost",
    "activities_Voided_qty": "Voided",
    "activities_Voided_cost": "Voided_Cost",
    "activities_Purchase_qty": "Purchase",
    "activities_Purchase_cost": "Purchase_Cost",
    "activities_Audit_qty": "Audit_Variance",
    "activities_Audit_cost": "Audit_Variance_Cost",
    "activities_Adjustment_qty": "Adjustment",
    "activities_Adjustment_cost": "Adjustment_Cost",
    "activities_Shrinkage_qty": "Shrinkage",
    "activities_Shrinkage_cost": "Shrinkage_Cost",
    "activities_Transfer In_qty": "Transfer_In",
    "activities_Transfer In_cost": "Transfer_In_Cost",
    "activities_Transfer Out_qty": "Transfer_Out",
    "activities_Transfer Out_cost": "Transfer_Out_Cost",
    "Housemade": "Housemade",
    "Housemade_Cost": "Housemade_Cost",
    "House_Consumed_Cost": "House_Consumed_Cost",
    "House_Consumed": "House_Consumed",
}

DESIRED_ORDER = [
    "Date",
    "Store_Name",
    "Store_Code",
    "Branch_Labels",
    "SKU",
    "TYPE",
    "Category",
    "NAME",
    "MEASURING_UNIT",
    "Opening_Balance",
    "Opening_Cost",
    "Closing_Balance",
    "Closing_Cost",
    "Activity",
    "Yield",
    "Sales",
    "Sales_Cost",
    "Returns",
    "Returns_Cost",
    "Voided",
    "Voided_Cost",
    "Purchase",
    "Purchase_Cost",
    "Audit_Variance",
    "Audit_Variance_Cost",
    "Adjustment",
    "Adjustment_Cost",
    "Shrinkage",
    "Shrinkage_Cost",
    "Transfer_In",
    "Transfer_In_Cost",
    "Transfer_Out",
    "Transfer_Out_Cost",
    "Housemade",
    "Housemade_Cost",
    "House_Consumed",
    "House_Consumed_Cost",
]

NUMERIC_COLUMNS = {
    "Opening_Balance",
    "Opening_Cost",
    "Closing_Balance",
    "Closing_Cost",
    "Yield",
    "Sales",
    "Sales_Cost",
    "Returns",
    "Returns_Cost",
    "Voided",
    "Voided_Cost",
    "Purchase",
    "Purchase_Cost",
    "Audit_Variance",
    "Audit_Variance_Cost",
    "Adjustment",
    "Adjustment_Cost",
    "Shrinkage",
    "Shrinkage_Cost",
    "Transfer_In",
    "Transfer_In_Cost",
    "Transfer_Out",
    "Transfer_Out_Cost",
    "Housemade",
    "Housemade_Cost",
    "House_Consumed",
    "House_Consumed_Cost",
}

# ==============================
# Config and logging
# ==============================

@dataclass(frozen=True)
class Config:
    days_back: int
    exclude_today: bool
    timezone: str
    max_workers: int
    request_timeout_seconds: int
    rista_max_retries: int
    max_failure_percent: float

    rista_api_key: str
    rista_api_secret: str
    rista_api_base_url: str

    load_to_azure: bool
    azure_account_url: str
    azure_sas_token: str
    azure_container: str
    azure_directory: str


@dataclass
class FetchResult:
    df: pd.DataFrame
    attempts: int
    failures: int


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def env_str(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return "" if value is None else str(value).strip()


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value == "" else int(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value is None or value == "" else float(value)


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def load_config() -> Config:
    """Build the run config from the environment.

    Credentials and account identifiers deliberately have no defaults: a missing
    value must fail loudly rather than silently target the wrong account.
    """
    load_dotenv()
    cfg = Config(
        days_back=env_int("DAYS_BACK", 5),
        exclude_today=env_bool("EXCLUDE_TODAY", True),
        timezone=env_str("TIMEZONE", "UTC"),
        max_workers=env_int("MAX_WORKERS", 6),
        request_timeout_seconds=env_int("REQUEST_TIMEOUT_SECONDS", 120),
        rista_max_retries=env_int("RISTA_MAX_RETRIES", 5),
        max_failure_percent=env_float("MAX_FAILURE_PERCENT", 10.0),
        rista_api_key=env_str("RISTA_API_KEY", required=True),
        rista_api_secret=env_str("RISTA_API_SECRET", required=True),
        rista_api_base_url=env_str("RISTA_API_BASE_URL", required=True).rstrip("/"),
        load_to_azure=env_bool("LOAD_TO_AZURE", True),
        azure_account_url=env_str("AZURE_ACCOUNT_URL", ""),
        azure_sas_token=env_str("AZURE_SAS_TOKEN", ""),
        azure_container=env_str("AZURE_CONTAINER", ""),
        azure_directory=env_str("AZURE_DIRECTORY", "Item_Balance_Activity_Cost").strip("/"),
    )

    if cfg.load_to_azure:
        for name, value in {
            "AZURE_ACCOUNT_URL": cfg.azure_account_url,
            "AZURE_SAS_TOKEN": cfg.azure_sas_token,
            "AZURE_CONTAINER": cfg.azure_container,
        }.items():
            if not value:
                raise ValueError(f"{name} is required when LOAD_TO_AZURE=true")

    return cfg

# ==============================
# Date helpers
# ==============================

def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def get_date_window(
    cfg: Config,
    start_date: str | None = None,
    end_date: str | None = None,
    days_back_override: int | None = None,
) -> tuple[dt.date, dt.date, list[dt.date]]:
    """Resolve the business-date window to load.

    Either an explicit --start-date/--end-date pair, or a rolling DAYS_BACK
    window in the configured timezone. Rolling windows normally exclude today,
    because the current business day is still accumulating activity.
    """
    if start_date or end_date:
        if not start_date or not end_date:
            raise ValueError("Pass both --start-date and --end-date, or neither.")
        start = parse_date(start_date)
        end = parse_date(end_date)
    else:
        days_back = days_back_override or cfg.days_back
        if days_back < 1:
            raise ValueError("days_back must be >= 1")
        today = dt.datetime.now(ZoneInfo(cfg.timezone)).date()
        if cfg.exclude_today:
            start = today - dt.timedelta(days=days_back)
            end = today - dt.timedelta(days=1)
        else:
            start = today - dt.timedelta(days=days_back - 1)
            end = today

    if end < start:
        raise ValueError("end date cannot be earlier than start date")

    dates = [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]
    return start, end, dates

# ==============================
# Rista API helpers
# ==============================

def canon_code(value: Any) -> str:
    return "" if value is None else str(value).strip().upper()


def create_token(cfg: Config) -> str:
    """Mint a short-lived HS256 JWT. The API expects the key as the issuer and
    the secret as the signing key, alongside the raw key in a header."""
    payload = {"iss": cfg.rista_api_key, "iat": dt.datetime.now(dt.timezone.utc)}
    token = jwt.encode(payload=payload, key=cfg.rista_api_secret, algorithm="HS256")
    return token.decode("utf-8") if isinstance(token, bytes) else token


def make_rista_session(cfg: Config) -> requests.Session:
    """One pooled session with transport-level retry on throttling and 5xx."""
    retry = Retry(
        total=cfg.rista_max_retries,
        connect=cfg.rista_max_retries,
        read=cfg.rista_max_retries,
        status=cfg.rista_max_retries,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=cfg.max_workers * 2, pool_maxsize=cfg.max_workers * 2)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def rista_headers(cfg: Config) -> dict[str, str]:
    return {
        "x-api-key": cfg.rista_api_key,
        "x-api-token": create_token(cfg),
        "content-type": "application/json",
    }


def fetch_store_list(session: requests.Session, cfg: Config) -> pd.DataFrame:
    """Discover the store list from the API rather than hardcoding branches, so
    new outlets are picked up without a code change."""
    url = f"{cfg.rista_api_base_url}/inventory/store/list"
    response = session.get(url, headers=rista_headers(cfg), timeout=cfg.request_timeout_seconds)
    response.raise_for_status()
    data = response.json() or []

    stores = []
    for item in data:
        store_code = canon_code(item.get("storeCode", ""))
        if not store_code:
            continue
        stores.append(
            {
                "StoreCode": store_code,
                "StoreName": "" if item.get("storeName") is None else str(item.get("storeName")),
            }
        )

    df = pd.DataFrame(stores).drop_duplicates(subset=["StoreCode"])
    if df.empty:
        raise RuntimeError("Rista store list returned no usable stores.")

    logging.info("Fetched %s stores from Rista.", len(df))
    return df


def fetch_activity_for_branch_date(
    session: requests.Session,
    cfg: Config,
    store_code: str,
    business_date: dt.date,
) -> tuple[list[dict[str, Any]], bool]:
    """Fetch one branch/date, following the API's `lastKey` cursor to exhaustion.

    Returns (rows, ok). A failed branch returns ok=False instead of raising, so
    one bad branch cannot abort the whole window - the caller decides whether
    the aggregate failure rate is tolerable.
    """
    url = f"{cfg.rista_api_base_url}/inventory/item/activity"
    date_str = business_date.isoformat()
    last_key = None
    rows_out: list[dict[str, Any]] = []

    while True:
        params: dict[str, Any] = {"branch": store_code, "day": date_str}
        if last_key:
            params["lastKey"] = last_key

        try:
            response = session.get(
                url,
                params=params,
                headers=rista_headers(cfg),
                timeout=cfg.request_timeout_seconds,
            )
        except requests.RequestException as exc:
            logging.warning("%s | %s | request error: %s", date_str, store_code, exc)
            return rows_out, False

        if response.status_code != 200:
            logging.warning("%s | %s | HTTP %s: %s", date_str, store_code, response.status_code, response.text[:300])
            return rows_out, False

        try:
            payload = response.json() or {}
        except ValueError as exc:
            logging.warning("%s | %s | invalid JSON: %s", date_str, store_code, exc)
            return rows_out, False

        rows = payload.get("data", []) or []
        for row in rows:
            if isinstance(row, dict):
                row["StoreCode"] = store_code
                row["Date"] = date_str
                rows_out.append(row)

        last_key = payload.get("lastKey")
        if not last_key:
            break

    return rows_out, True


def fetch_activity_for_date(
    session: requests.Session,
    cfg: Config,
    branchlist: Iterable[str],
    business_date: dt.date,
) -> FetchResult:
    """Fan out across branches for a single business date."""
    branch_codes = list(branchlist)
    all_rows: list[dict[str, Any]] = []
    attempts = len(branch_codes)
    failures = 0

    logging.info("Fetching IBAC for %s across %s branches.", business_date.isoformat(), attempts)

    with ThreadPoolExecutor(max_workers=cfg.max_workers) as executor:
        futures = {
            executor.submit(fetch_activity_for_branch_date, session, cfg, code, business_date): code
            for code in branch_codes
        }
        for future in as_completed(futures):
            code = futures[future]
            try:
                rows, ok = future.result()
                if not ok:
                    failures += 1
                all_rows.extend(rows)
            except Exception as exc:  # defensive guard for unexpected failures
                failures += 1
                logging.exception("%s | %s | unexpected fetch error: %s", business_date.isoformat(), code, exc)

    df = pd.DataFrame(all_rows)
    logging.info(
        "Completed %s | rows=%s | failed_branches=%s/%s",
        business_date.isoformat(),
        len(df),
        failures,
        attempts,
    )
    return FetchResult(df=df, attempts=attempts, failures=failures)


def fetch_data_for_window(session: requests.Session, cfg: Config, branchlist: Iterable[str], dates: list[dt.date]) -> FetchResult:
    frames: list[pd.DataFrame] = []
    total_attempts = 0
    total_failures = 0

    for business_date in dates:
        result = fetch_activity_for_date(session, cfg, branchlist, business_date)
        total_attempts += result.attempts
        total_failures += result.failures
        if not result.df.empty:
            frames.append(result.df)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    logging.info("Fetch summary | rows=%s | failures=%s/%s", len(combined), total_failures, total_attempts)
    return FetchResult(df=combined, attempts=total_attempts, failures=total_failures)


def validate_fetch_quality(result: FetchResult, cfg: Config) -> None:
    """Circuit breaker.

    The load is a destructive partition replace, so a partially-successful fetch
    must never overwrite good data with a subset of it.
    """
    if result.attempts == 0:
        raise RuntimeError("No API attempts were made.")

    failure_percent = (result.failures / result.attempts) * 100
    if failure_percent > cfg.max_failure_percent:
        raise RuntimeError(
            f"Aborting load: API failure rate {failure_percent:.2f}% is above "
            f"MAX_FAILURE_PERCENT={cfg.max_failure_percent:.2f}%."
        )

    if result.df.empty and result.failures > 0:
        raise RuntimeError("Aborting load: API returned zero rows and had failures. Existing data was not replaced.")

# ==============================
# Transformation
# ==============================

def flatten_activities(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot the nested `activities` array into one qty/cost column pair per
    activity type (Sales, Purchase, Shrinkage, Transfer In/Out, ...).

    Pivoting rather than exploding keeps the output at one row per
    date/store/item, which is the grain the warehouse models expect.
    """
    if df.empty:
        return df

    out = df.copy()
    out["_row_id"] = np.arange(len(out))

    if "activities" not in out.columns:
        return out.drop(columns=["_row_id"], errors="ignore")

    out["activities"] = out["activities"].apply(lambda x: x if isinstance(x, list) else [])
    exploded = out[["_row_id", "activities"]].explode("activities", ignore_index=False)
    exploded = exploded[exploded["activities"].notna()]

    if exploded.empty:
        return out.drop(columns=["activities", "_row_id"], errors="ignore")

    base = exploded.drop(columns=["activities"]).reset_index(drop=True)
    norm = pd.json_normalize(exploded["activities"].reset_index(drop=True))
    exploded = pd.concat([base, norm], axis=1)

    if "type" not in exploded.columns:
        return out.drop(columns=["activities", "_row_id"], errors="ignore")

    exploded["type"] = exploded["type"].astype(str)
    for col in ("quantity", "cost"):
        if col not in exploded.columns:
            exploded[col] = 0.0
        exploded[col] = pd.to_numeric(exploded[col], errors="coerce").fillna(0.0)

    qty_wide = (
        exploded.pivot_table(index="_row_id", columns="type", values="quantity", aggfunc="sum")
        .add_prefix("activities_")
        .add_suffix("_qty")
    )
    cost_wide = (
        exploded.pivot_table(index="_row_id", columns="type", values="cost", aggfunc="sum")
        .add_prefix("activities_")
        .add_suffix("_cost")
    )

    wide = pd.concat([qty_wide, cost_wide], axis=1).reset_index()
    out = out.drop(columns=["activities"]).merge(wide, on="_row_id", how="left").drop(columns=["_row_id"])

    base_cols = [col for col in out.columns if not col.startswith("activities_")]
    activity_cols = sorted([col for col in out.columns if col.startswith("activities_")])
    out = out[base_cols + activity_cols]

    for col in ["Housemade", "Housemade_Cost", "House_Consumed", "House_Consumed_Cost", "Yield", "Returns", "Returns_Cost"]:
        if col not in out.columns:
            out[col] = 0

    return out


def transform_ibac(fact_raw: pd.DataFrame, stores: pd.DataFrame) -> pd.DataFrame:
    """Flatten, rename to the warehouse contract, and enforce a stable schema.

    The output columns are always DESIRED_ORDER, in that order, regardless of
    which activity types the API happened to return for the window. A stable
    Parquet schema is what lets the downstream COPY INTO stay unchanged.
    """
    if fact_raw.empty:
        return pd.DataFrame(columns=DESIRED_ORDER)

    name_map = dict(zip(stores["StoreCode"], stores["StoreName"]))
    fact = flatten_activities(fact_raw)
    fact["StoreCode"] = fact["StoreCode"].apply(canon_code)
    fact["StoreName"] = fact["StoreCode"].map(name_map).fillna("")

    # Central stores are identified by a naming convention on the store record.
    fact["StoreLabel"] = np.where(
        fact["StoreName"].astype(str).str.contains("warehouse", case=False, na=False),
        "Central Store",
        "Outlet",
    )

    out = fact.rename(columns=COLUMN_MAPPING)

    for col in DESIRED_ORDER:
        if col not in out.columns:
            out[col] = 0 if col in NUMERIC_COLUMNS else ""

    out = out[DESIRED_ORDER].copy()

    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.date
    out = out[out["Date"].notna()].copy()

    for col in NUMERIC_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).astype(float)

    string_cols = [col for col in DESIRED_ORDER if col not in NUMERIC_COLUMNS and col != "Date"]
    for col in string_cols:
        out[col] = out[col].where(pd.notna(out[col]), "").astype(str)

    return out

# ==============================
# Azure Blob upload
# ==============================

def get_blob_service_client(cfg: Config) -> BlobServiceClient:
    sas_token = cfg.azure_sas_token.lstrip("?")
    return BlobServiceClient(account_url=cfg.azure_account_url, credential=sas_token)


def delete_blob_if_exists(container_client: Any, blob_name: str) -> None:
    try:
        container_client.get_blob_client(blob_name).delete_blob(delete_snapshots="include")
        logging.info("Deleted existing blob: %s", blob_name)
    except ResourceNotFoundError:
        logging.info("No existing blob to delete: %s", blob_name)


def df_to_arrow_all_strings(df: pd.DataFrame) -> pa.Table:
    """Write every column as a string.

    Typing is deferred to the dbt bronze layer, so an upstream type change in
    the API can never fail the load or silently coerce a value at write time.
    """
    arrays = []
    names = list(df.columns)
    for col in names:
        series = df[col].where(pd.notna(df[col]), None)
        values = [None if value is None else str(value) for value in series.tolist()]
        arrays.append(pa.array(values, type=pa.string()))
    return pa.Table.from_arrays(arrays, names=names)


def upload_parquet(container_client: Any, df: pd.DataFrame, blob_name: str) -> None:
    table = df_to_arrow_all_strings(df)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy", use_dictionary=True)
    buffer.seek(0)

    container_client.upload_blob(
        name=blob_name,
        data=buffer.getvalue(),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/octet-stream"),
    )


def save_partitions_to_blob(cfg: Config, df: pd.DataFrame, dates: list[dt.date]) -> None:
    """Replace one blob per business date: `business_date=YYYY-MM-DD.parquet`.

    Idempotent by construction - re-running a window overwrites the same blob
    names instead of appending duplicates. A date that legitimately returned no
    rows still has its stale blob deleted, so deletions upstream propagate.
    """
    if not cfg.load_to_azure:
        logging.info("Skipping Azure upload because LOAD_TO_AZURE=false.")
        return

    blob_service = get_blob_service_client(cfg)
    container_client = blob_service.get_container_client(cfg.azure_container)

    upload_df = df.copy()
    if not upload_df.empty:
        upload_df["Date"] = pd.to_datetime(upload_df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    logging.info("Replacing Azure partitions for %s dates.", len(dates))
    for business_date in dates:
        date_str = business_date.isoformat()
        blob_name = f"{cfg.azure_directory}/business_date={date_str}.parquet"
        delete_blob_if_exists(container_client, blob_name)

        part = upload_df[upload_df["Date"] == date_str].copy() if not upload_df.empty else pd.DataFrame()
        if part.empty:
            logging.info("No rows for %s. Deleted old blob and skipped upload.", date_str)
            continue

        upload_parquet(container_client, part[DESIRED_ORDER], blob_name)
        logging.info("Uploaded %s | rows=%s", blob_name, len(part))

    logging.info("Azure upload complete.")

# ==============================
# Main execution
# ==============================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Rista IBAC data and land Parquet partitions in Azure Blob.")
    parser.add_argument("--days-back", type=int, default=None, help="Override DAYS_BACK from the environment.")
    parser.add_argument("--start-date", type=str, default=None, help="Inclusive start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", type=str, default=None, help="Inclusive end date, YYYY-MM-DD.")
    parser.add_argument("--skip-azure", action="store_true", help="Fetch and transform only. Writes nothing (dry run).")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    cfg = load_config()

    if args.skip_azure:
        cfg = Config(**{**cfg.__dict__, "load_to_azure": False})

    start, end, dates = get_date_window(
        cfg,
        start_date=args.start_date,
        end_date=args.end_date,
        days_back_override=args.days_back,
    )
    logging.info("Date window: %s to %s inclusive | timezone=%s", start, end, cfg.timezone)

    session = make_rista_session(cfg)
    stores = fetch_store_list(session, cfg)
    branchlist = stores["StoreCode"].dropna().tolist()

    fetch_result = fetch_data_for_window(session, cfg, branchlist, dates)
    validate_fetch_quality(fetch_result, cfg)

    out = transform_ibac(fetch_result.df, stores)
    logging.info("Transformed output rows=%s | columns=%s", len(out), len(out.columns))

    save_partitions_to_blob(cfg, out, dates)

    logging.info("IBAC pipeline complete.")


if __name__ == "__main__":
    main()
