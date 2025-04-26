# main.py
import os
from dotenv import load_dotenv
load_dotenv()

PORT = int(os.getenv("PORT", 8000))

import os
import uuid
import json
from datetime import datetime
from pathlib import Path

import asyncio
import aiohttp
import logging

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from parser import (
    load_env,
    initialize_mappings,
    fetch_emails,
    process_email_with_delay,
    save_to_csv,
    save_failed_emails_to_csv,
)

# Load environment
env = load_env()

# FastAPI app
app = FastAPI()

# Allow CORS from frontend url (dynamic based on env or fallback)
frontend_origin = os.getenv("FRONTEND_URL", "http://localhost:5173")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[frontend_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("main")

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Track processes
process_statuses = {}

# Init mappings
initialize_mappings()

@app.post("/start-process")
async def start_process(background_tasks: BackgroundTasks):
    process_id = str(uuid.uuid4())
    process_statuses[process_id] = {
        "status": "fetching_emails",
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

async def run_processing(process_id: str):
    try:
        emails = await fetch_emails(env, process_id)
        process_statuses[process_id]["email_count"] = len(emails)

        if not emails:
            process_statuses[process_id]["status"] = "error"
            process_statuses[process_id]["error"] = "No emails found."
            return

        output_file = f"parsed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        total_rows = 0
        failed_emails = []

        async with aiohttp.ClientSession() as session:
            for idx, email in enumerate(emails):
                process_statuses[process_id]["current_email"] = idx + 1
                valid_rows, skipped_rows, failed_email = await process_email_with_delay(
                    email, env, process_id, session
                )

                if valid_rows:
                    save_to_csv(valid_rows, output_file, process_id)
                    total_rows += len(valid_rows)
                    process_statuses[process_id]["row_count"] = total_rows

                if failed_email:
                    failed_email["content"] = email.get("content", "")
                    failed_emails.append(failed_email)

                await asyncio.sleep(2)

            if failed_emails:
                logger.info(f"Retrying {len(failed_emails)} failed emails...")
                for failed in failed_emails[:]:
                    retry_email = {
                        "uid": failed.get("email_id", "?"),
                        "subject": failed.get("subject", ""),
                        "from_addr": failed.get("from_addr", ""),
                        "content": failed.get("content", ""),
                    }
                    valid_rows, skipped_rows, retry_failed = await process_email_with_delay(
                        retry_email, env, process_id, session
                    )
                    if valid_rows:
                        save_to_csv(valid_rows, output_file, process_id)
                        total_rows += len(valid_rows)
                        process_statuses[process_id]["row_count"] = total_rows
                        failed_emails.remove(failed)

        save_failed_emails_to_csv(failed_emails, output_file, process_id)

        process_statuses[process_id]["status"] = "done"
        process_statuses[process_id]["output_file"] = output_file

    except Exception as ex:
        logger.error(f"Processing error: {str(ex)}")
        process_statuses[process_id]["status"] = "error"
        process_statuses[process_id]["error"] = str(ex)
