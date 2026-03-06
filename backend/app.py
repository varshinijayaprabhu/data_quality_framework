from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os
import sys
from urllib.parse import urlparse

from dotenv import load_dotenv
from main import run_pipeline

load_dotenv()

# Allow importing src modules
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

app = FastAPI(
    title="Gesix Data Quality API",
    description="The central API for Ingestion, Validation, and Remediation. Features auto-generated Swagger documentation.",
    version="1.0.0"
)

# CORS Configuration
origins = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
CLEANED_PARQUET = os.path.join(PROCESSED_DIR, "cleaned_data.parquet")
CLEANED_CSV = os.path.join(PROCESSED_DIR, "cleaned_data.csv")

# Mount React Frontend if it exists
FRONTEND_DIR = os.path.join(os.path.dirname(BASE_DIR), "frontend", "dist")
if os.path.exists(FRONTEND_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(FRONTEND_DIR, "assets")), name="assets")

def validate_source_url(url: str) -> bool:
    """Security check: Only allow HTTPS URLs to prevent SSRF and protocol attacks."""
    if not url:
        return True  # Allow None/empty for file uploads
    
    try:
        parsed = urlparse(url)
        # Only allow HTTPS protocol
        if parsed.scheme != 'https':
            return False
        return True
    except Exception:
        return False

def get_latest_raw_data():
    """Returns the most recent data - prefers cleaned data if available."""
    import pandas as pd
    
    # Prefer cleaned data if it exists (after remediation ran)
    if os.path.exists(CLEANED_PARQUET):
        hub_path = CLEANED_PARQUET
    elif os.path.exists(os.path.join(PROCESSED_DIR, "raw_structured.parquet")):
        hub_path = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    else:
        return {"data": []}
            
    print(f"[*] Serving dataset preview from: {hub_path}")
    
    try:
        df = pd.read_parquet(hub_path)
        if len(df) > 100: 
            df = df.head(100)
        
        # Handle BOTH NaN and empty strings as missing
        df = df.fillna("—")
        # Also replace empty strings and whitespace-only strings
        for col in df.select_dtypes(include=['object', 'string']).columns:
            df[col] = df[col].apply(lambda x: "—" if isinstance(x, str) and x.strip() == "" else x)
        
        import json
        clean_records = json.loads(df.to_json(orient="records"))
        
        return {"data": clean_records}
    except Exception as e:
        print(f"[!] Error reading parquet hub for preview: {e}")
        return {"data": []}

def get_raw_and_cleaned_data():
    """Returns both raw_structured and cleaned_data separately."""
    import pandas as pd
    import json
    
    result = {
        "raw_data": [],
        "cleaned_data": []
    }
    
    RAW_STRUCTURED = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    
    # Get raw_structured data
    if os.path.exists(RAW_STRUCTURED):
        try:
            df = pd.read_parquet(RAW_STRUCTURED)
            if len(df) > 100:
                df = df.head(100)
            df = df.fillna("—")
            for col in df.select_dtypes(include=['object', 'string']).columns:
                df[col] = df[col].apply(lambda x: "—" if isinstance(x, str) and x.strip() == "" else x)
            result["raw_data"] = json.loads(df.to_json(orient="records"))
            print(f"[*] Raw data: {len(result['raw_data'])} records")
        except Exception as e:
            print(f"[!] Error reading raw_structured: {e}")
    
    # Get cleaned_data
    if os.path.exists(CLEANED_PARQUET):
        try:
            df = pd.read_parquet(CLEANED_PARQUET)
            if len(df) > 100:
                df = df.head(100)
            df = df.fillna("—")
            for col in df.select_dtypes(include=['object', 'string']).columns:
                df[col] = df[col].apply(lambda x: "—" if isinstance(x, str) and x.strip() == "" else x)
            result["cleaned_data"] = json.loads(df.to_json(orient="records"))
            print(f"[*] Cleaned data: {len(result['cleaned_data'])} records")
        except Exception as e:
            print(f"[!] Error reading cleaned_data: {e}")
    
    return result

def get_report_json():
    """Run validator on cleaned_data.csv and return report as dict, or None if no data."""
    try:
        from qa.validator import DataValidator
        validator = DataValidator()
        report = validator.validate(CLEANED_PARQUET)
        return report
    except Exception as e:
        print(f"Validation Error: {e}")
        return {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}

def get_both_reports():
    """Run validator on both raw_structured and cleaned_data, return both reports."""
    from qa.validator import DataValidator
    
    RAW_STRUCTURED = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    
    raw_report = None
    cleaned_report = None
    
    validator = DataValidator()
    
    # Validate raw data
    if os.path.exists(RAW_STRUCTURED):
        try:
            print("[*] Running validation on RAW data...")
            raw_report = validator.validate(RAW_STRUCTURED)
        except Exception as e:
            print(f"Raw Validation Error: {e}")
            raw_report = {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}
    
    # Validate cleaned data
    if os.path.exists(CLEANED_PARQUET):
        try:
            print("[*] Running validation on CLEANED data...")
            cleaned_report = validator.validate(CLEANED_PARQUET)
        except Exception as e:
            print(f"Cleaned Validation Error: {e}")
            cleaned_report = {"error": str(e), "status": "Error", "total_records": 0, "overall_trustability": 0, "dimensions": {}}
    
    return {
        "raw_report": raw_report,
        "cleaned_report": cleaned_report
    }

@app.get("/api/report", summary="Get Latest Quality Report")
async def api_report():
    """Return latest quality report as JSON for the React frontend."""
    report = get_report_json()
    if not report or report.get("error"):
        return JSONResponse(status_code=500, content=report or {"error": "Report not found"})
    return report

@app.get("/api/raw-data", summary="Get Raw Data Preview")
async def get_raw_data_endpoint():
    """Returns the raw ingested data (for table preview)."""
    return get_latest_raw_data()

@app.post("/api/process", summary="Trigger Data Quality Pipeline")
async def api_process(
    source_type: str = Form("api"),
    source_url: str = Form(None),
    start_date: str = Form(None),
    end_date: str = Form(None),
    api_key: str = Form(None),
    file: UploadFile = File(None)
):
    """Run the pipeline with dynamic source selection and return report JSON."""
    file_path = None
    
    # Handle File Upload Paths
    if source_type in ["upload", "pdf", "docx", "json_upload", "xlsx_upload", "zip_upload", "xml_upload", "parquet_upload", "others_upload"]:
        if not file or not file.filename:
            return JSONResponse(status_code=400, content={"success": False, "error": f"No {source_type.upper()} file uploaded"})
        
        temp_dir = os.path.join(BASE_DIR, "data", "temp")
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            
        file_path = os.path.join(temp_dir, file.filename)
        with open(file_path, "wb") as f:
            f.write(await file.read())
        print(f"File uploaded to {file_path}")

    print(f"API trigger: Source={source_type}, URL={source_url}, Dates={start_date} to {end_date}")
    
    # Security: Validate URL for API/scraper sources
    if source_type in ["api", "scraper"] and source_url:
        if not validate_source_url(source_url):
            return JSONResponse(
                status_code=400, 
                content={
                    "success": False, 
                    "error": "Security Error: Only HTTPS URLs are allowed. HTTP and other protocols are blocked for security."
                }
            )
    
    try:
        report = run_pipeline(
            start_date=start_date, 
            end_date=end_date, 
            source_type=source_type, 
            source_url=source_url,
            file_path=file_path,
            api_key=api_key
        )
        
        is_success = True
        status = report.get("status") if isinstance(report, dict) else "Unknown"
        fail_statuses = ["Unification Failed", "Remediation Failed", "QA Failed", "Ingestion Failed", "Cleanup Failed"]
        if isinstance(report, dict) and (status in fail_statuses or not report):
            is_success = False

        if is_success and status != "No Data Found for this period":
            both_data = get_raw_and_cleaned_data()
            both_reports = get_both_reports()
        else:
            both_data = {"raw_data": [], "cleaned_data": []}
            both_reports = {"raw_report": None, "cleaned_report": None}
        
        response_data = {
            "success": is_success,
            "report": report,  # Keep for backward compatibility (cleaned report)
            "raw_report": both_reports.get("raw_report"),
            "cleaned_report": both_reports.get("cleaned_report"),
            "raw_data": {"data": both_data["raw_data"]},
            "cleaned_data": {"data": both_data["cleaned_data"]},
            "error": report.get("error") if not is_success else None
        }
        
        print(f"API Response: Success={is_success}, Status={status}")
        return response_data
    except Exception as e:
        import traceback
        print(f"Pipeline Execution Error: {e}\n{traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "error": f"CANARY ERROR: {str(e)}"})
    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

@app.get("/legacy-dashboard", summary="Serve old dashboard")
async def index():
    """Serve legacy HTML dashboard if requested."""
    html_path = os.path.join(PROCESSED_DIR, "dashboard.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    raise HTTPException(status_code=404, detail="Legacy dashboard not found")


# EDA Profile for Cleaned Data
@app.get("/api/eda-profile", summary="Serve EDA Profile Report (Cleaned Data)")
async def get_eda_profile():
    profile_path = os.path.join(PROCESSED_DIR, "eda_profile.html")
    if os.path.exists(profile_path):
        return FileResponse(profile_path, media_type="text/html")
    if os.path.exists(CLEANED_PARQUET):
        try:
            import pandas as pd
            from reporting.profiler import DataProfiler
            print("[*] Cleaned EDA Profile not found — generating on-the-fly...")
            df = pd.read_parquet(CLEANED_PARQUET)
            profiler = DataProfiler()
            result = profiler.generate(df, title="Gesix EDA Profile (Cleaned Data)")
            if result and os.path.exists(profile_path):
                return FileResponse(profile_path, media_type="text/html")
        except Exception as e:
            print(f"[!] On-the-fly EDA generation failed: {e}")
            raise HTTPException(status_code=500, detail=f"EDA Profile generation failed: {str(e)}")
    raise HTTPException(status_code=404, detail="No cleaned data available. Run an analysis first.")

# EDA Profile for Raw Data
@app.get("/api/eda-profile-raw", summary="Serve EDA Profile Report (Raw Data)")
async def get_eda_profile_raw():
    raw_profile_path = os.path.join(PROCESSED_DIR, "eda_profile_raw.html")
    raw_parquet = os.path.join(PROCESSED_DIR, "raw_structured.parquet")
    if os.path.exists(raw_profile_path):
        return FileResponse(raw_profile_path, media_type="text/html")
    if os.path.exists(raw_parquet):
        try:
            import pandas as pd
            from reporting.profiler import DataProfiler
            print("[*] Raw EDA Profile not found — generating on-the-fly...")
            df = pd.read_parquet(raw_parquet)
            profiler = DataProfiler()
            # Save as eda_profile_raw.html
            result = profiler.generate(df, title="Gesix EDA Profile (Raw Data)")
            # Rename/move output if needed
            default_profile_path = os.path.join(PROCESSED_DIR, "eda_profile.html")
            if result == default_profile_path and os.path.exists(default_profile_path):
                os.rename(default_profile_path, raw_profile_path)
            if os.path.exists(raw_profile_path):
                return FileResponse(raw_profile_path, media_type="text/html")
        except Exception as e:
            print(f"[!] On-the-fly RAW EDA generation failed: {e}")
            raise HTTPException(status_code=500, detail=f"Raw EDA Profile generation failed: {str(e)}")
    raise HTTPException(status_code=404, detail="No raw data available. Run an analysis first.")

@app.get("/{path:path}", include_in_schema=False)
async def catch_all(path: str):
    """Catch-all route to serve index.html for React SPA routing."""
    if os.path.exists(FRONTEND_DIR):
        file_path = os.path.join(FRONTEND_DIR, path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path)
        
        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
            
    return JSONResponse(
        status_code=404, 
        content={"message": "Dashboard not found. Ensure the frontend is built: `cd frontend && npm run build`"}
    )



if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    host = os.environ.get("HOST", "0.0.0.0")

    print("\n" + "=" * 50)
    print(f"Gesix Data Quality Production Server (FastAPI)")
    print(f"API Swagger UI: http://localhost:{port}/docs")
    print(f"Frontend App:   http://localhost:{port}")
    print("=" * 50 + "\n")

    uvicorn.run("app:app", host=host, port=port, reload=True)
