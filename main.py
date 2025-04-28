# main.py
import asyncio
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from parser import process_all_emails, initialize_mappings, load_process_status, delete_process_status
from uuid import uuid4
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize mappings at startup
@app.on_event("startup")
async def startup_event():
    logger.info("Starting up application and initializing mappings")
    initialize_mappings()

# Simple endpoint to test CORS without background tasks
@app.get("/cors-test")
async def cors_test():
    logger.debug("Received GET request to /cors-test")
    return {"message": "CORS test successful"}

@app.options("/start-process")
async def handle_options_start_process():
    logger.debug("Handling OPTIONS request for /start-process")
    return {"status": "ok"}

@app.post("/start-process")
async def start_process(background_tasks: BackgroundTasks):
    """
    Start processing emails asynchronously and immediately return process_id.
    """
    logger.debug("Received POST request to /start-process")
    process_id = str(uuid4())
    logger.debug(f"Generated process_id: {process_id}")

    # Launch background task to process emails
    background_tasks.add_task(process_all_emails, process_id)
    logger.info("Background task started for email processing.")

    # Immediately return the process ID (no sleep)
    response = {"process_id": process_id}
    logger.debug(f"Returning response: {response}")
    return response

@app.get("/status/{process_id}")
async def get_status(process_id: str):
    """
    Get current status of a background parsing task.
    """
    logger.debug(f"Received GET request to /status/{process_id}")
    status = load_process_status(process_id)
    if not status:
        logger.warning(f"Process {process_id} not found")
        raise HTTPException(status_code=404, detail="Process not found")
    return status

@app.get("/download/{filename}")
async def download_file(filename: str):
    """
    Download output CSV or failed CSV by filename.
    """
    logger.debug(f"Received GET request to /download/{filename}")
    file_path = Path("output") / filename
    if not file_path.exists():
        logger.warning(f"File {filename} not found")
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=filename)

@app.get("/keep-alive")
async def keep_alive():
    """
    Simple keep-alive endpoint for frontend pinging.
    """
    logger.debug("Received GET request to /keep-alive")
    return {"status": "alive"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
