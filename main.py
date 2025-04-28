from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from parser import (
    process_all_emails,
    initialize_mappings,
    load_process_status,
    delete_process_status,  # Ensure this is imported
    load_env
)
from uuid import uuid4
from pathlib import Path
import logging

# Load environment variables
load_env()  # Ensure environment variables are loaded

app = FastAPI()

# Initialize mappings at startup
@app.on_event("startup")
async def startup_event():
    initialize_mappings()

@app.post("/start-process")
async def start_process(background_tasks: BackgroundTasks):
    """
    Start processing emails asynchronously and immediately return process_id.
    """
    process_id = str(uuid4())
    background_tasks.add_task(process_all_emails, process_id)
    logging.info("Your service is live 🎉")
    return {"process_id": process_id}

@app.get("/status/{process_id}")
async def get_status(process_id: str):
    """
    Get current status of a background parsing task.
    """
    status = load_process_status(process_id)
    if not status:
        raise HTTPException(status_code=404, detail="Process not found")
    return status

@app.get("/download/{filename}")
async def download_file(filename: str):
    """
    Download output CSV or failed CSV by filename.
    """
    file_path = Path("output") / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=filename)

@app.get("/keep-alive")
async def keep_alive():
    """
    Endpoint to keep the server alive during long-running tasks.
    """
    return {"status": "alive"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
