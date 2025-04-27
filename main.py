# main.py
import os
import uuid
import logging
import asyncio
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from parser import (
    load_env,
    initialize_mappings,
    process_all_emails,
)

# Load environment variables
load_dotenv()
env = load_env()

PORT = int(os.getenv("PORT", 8000))

# Configure FastAPI app
app = FastAPI()

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://bde-frontend-pf3m.onrender.com",
        "https://bde-project.onrender.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logger setup
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("main")

# Directory setup
BASE_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Initialize mappings at startup
initialize_mappings()

# Process Tracking
process_statuses = {}

# --- FastAPI Routes ---

@app.get("/")
async def root():
    return {"message": "BDE Parser API is running."}


@app.post("/start-process")
async def start_process(background_tasks: BackgroundTasks):
    process_id = str(uuid.uuid4())
    process_statuses[process_id] = {
        "status": "started",
        "email_count": 0,
        "current_email": 0,
        "row_count": 0,
        "output_file": "",
        "failed_emails": [],
        "error": "",
    }
    background_tasks.add_task(run_processing, process_id)
    return {"process_id": process_id}


@app.get("/status/{process_id}")
async def get_status(process_id: str):
    status = process_statuses.get(process_id)
    if not status:
        raise HTTPException(status_code=404, detail="Invalid process ID")
    return status


@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = OUTPUT_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=filename, media_type="text/csv")


# --- Background Email Processing Task ---

async def run_processing(process_id: str):
    try:
        await process_all_emails(process_id, process_statuses)
        logger.info(f"✅ Processing complete for process_id {process_id}")

        process_statuses[process_id]["status"] = "done"
        # Optionally: if process_all_emails doesn't set output_file itself
        # process_statuses[process_id]["output_file"] = output_file  # <-- already handled in process_all_emails

        await asyncio.sleep(10)  # ⚡ Wait a bit after marking as done so frontend can finish downloading

    except Exception as ex:
        logger.exception(f"❌ Processing error for process_id {process_id}: {str(ex)}")
        process_statuses[process_id]["status"] = "error"
        process_statuses[process_id]["error"] = str(ex)
