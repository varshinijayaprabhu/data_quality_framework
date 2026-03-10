import os
import uuid
import datetime
from dotenv import load_dotenv
from supabase import create_async_client, AsyncClient

load_dotenv()

# Singleton-like client
_supabase: AsyncClient | None = None

def ensure_utc(dt: datetime.datetime) -> datetime.datetime:
    """Ensure a datetime object is timezone-aware and set to UTC."""
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.datetime.fromisoformat(dt.replace('Z', '+00:00'))
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)

async def get_supabase() -> AsyncClient:
    global _supabase
    if _supabase is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set")
        _supabase = await create_async_client(url, key)
    return _supabase

async def close_pool() -> None:
    # Supabase HTTP client doesn't require explicit closing like a pool
    pass

async def ensure_schema() -> None:
    # Schema management is typically done via Supabase Dashboard/SQL Editor for REST API
    # We verify connectivity by doing a simple count
    try:
        supabase = await get_supabase()
        await supabase.table("file_metadata").select("id", count="exact").limit(1).execute()
        print("[*] Database: Connectivity verified via REST API.")
    except Exception as e:
        print(f"[!] Database connectivity check failed: {e}")

async def insert_file_metadata(metadata: dict) -> None:
    """Insert a row into the metadata table using Supabase REST API."""
    supabase = await get_supabase()
    
    # Prepare data for insertion (Supabase REST handles UUID and Timestamps as strings)
    data = {
        "id": str(metadata.get("id")),
        "file_name": metadata.get("file_name"),
        "file_path": metadata.get("file_path"),
        "file_type": metadata.get("file_type"),
        "source": metadata.get("source"),
        "status": metadata.get("status"),
        "size": metadata.get("size"),
        "upload_date": ensure_utc(metadata.get("upload_date")).isoformat(),
        "url": metadata.get("url"),
        "analysis_report_url": metadata.get("analysis_report_url"),
    }
    
    await supabase.table("file_metadata").insert(data).execute()

async def update_file_status_and_report(file_id: str, status: str, report_url: str) -> None:
    """Update status and report URL."""
    supabase = await get_supabase()
    await supabase.table("file_metadata").update({
        "status": status,
        "analysis_report_url": report_url
    }).eq("id", file_id).execute()

async def update_parquet_urls(file_id: str, raw_url: str, cleaned_url: str) -> None:
    """Update raw and cleaned parquet URLs."""
    supabase = await get_supabase()
    await supabase.table("file_metadata").update({
        "raw_parquet_url": raw_url,
        "cleaned_parquet_url": cleaned_url
    }).eq("id", file_id).execute()

async def update_eda_urls(file_id: str, raw_eda_url: str, cleaned_eda_url: str) -> None:
    """Update raw and cleaned EDA profile URLs."""
    supabase = await get_supabase()
    await supabase.table("file_metadata").update({
        "raw_eda_profile_url": raw_eda_url,
        "eda_profile_url": cleaned_eda_url
    }).eq("id", file_id).execute()

async def get_record_by_id(file_id: str):
    """Fetch record by ID (REST API). Supabase handles RLS and filtering."""
    supabase = await get_supabase()
    response = await supabase.table("file_metadata").select("*").eq("id", file_id).execute()
    
    if not response.data:
        return None
        
    record = response.data[0]
    
    # Check for 7-day expiry
    upload_date_str = record.get('upload_date')
    if upload_date_str:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        record_date = ensure_utc(datetime.datetime.fromisoformat(upload_date_str.replace('Z', '+00:00')))
        
        days_old = (now_utc - record_date).days
        if days_old >= 7:
            record['expired'] = True
            record['analysis_report_url'] = None
            record['eda_profile_url'] = None
            record['raw_eda_profile_url'] = None
        else:
            record['expired'] = False
    
    return record

async def get_expired_records(threshold_days: int = 7):
    """Find records older than N days for cleanup via REST filtering."""
    supabase = await get_supabase()
    threshold_date = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=threshold_days)).isoformat()
    
    response = await supabase.table("file_metadata")\
        .select("id, url, analysis_report_url, eda_profile_url, raw_eda_profile_url, raw_parquet_url, cleaned_parquet_url")\
        .lt("upload_date", threshold_date)\
        .neq("status", "purged")\
        .execute()
        
    return response.data

async def mark_as_purged(file_id: str):
    """Mark a record as purged."""
    supabase = await get_supabase()
    await supabase.table("file_metadata").update({
        "status": "purged",
        "analysis_report_url": None,
        "url": None
    }).eq("id", file_id).execute()
