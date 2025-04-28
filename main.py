import uuid
import logging
import asyncio  # Add this import
import aiofiles
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from parser import process_all_emails, process_status, load_process_status, logger

app = FastAPI()

# Configure logging
logger.setLevel(logging.INFO)

@app.post("/start-process")
async def start_process():
    process_id = str(uuid.uuid4())
    process_status[process_id] = {
        "status": "starting",
        "email_count": 0,
        "current_email": 0,
        "row_count": 0,
        "output_file": None,
        "error": None,
        "debug_log": f"debug_{process_id}.txt"
    }
    logger.info(f"Starting process {process_id}")
    # Start the process in the background
    asyncio.create_task(process_all_emails(process_id, process_status))
    return {"process_id": process_id}

@app.get("/status/{process_id}")
async def get_status(process_id: str):
    status = load_process_status(process_id)
    if status is None:
        status = process_status.get(process_id)
    if not status:
        raise HTTPException(status_code=404, detail="Process not found")
    return status

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = f"output/{filename}"
    try:
        async with aiofiles.open(file_path, mode='rb') as f:
            await f.read()  # Test if file exists
        return FileResponse(file_path, media_type='application/octet-stream', filename=filename)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")