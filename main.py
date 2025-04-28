from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from parser import (
    process_all_emails,
    initialize_mappings,
    load_process_status,
    delete_process_status,
    load_env,
    process_status  # Import process_status
)
from uuid import uuid4
from pathlib import Path
import logging

# Load environment variables
load_env()  # Ensure environment variables are loaded

# Initialize FastAPI app
app = FastAPI()

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logger = logging.getLogger(__name__)

# CORS configuration to allow requests from your frontend domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://bde-frontend-pf3m.onrender.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize mappings at startup
@app.on_event("startup")
async def startup_event():
    try:
        initialize_mappings()
        logger.info("Successfully initialized mappings")
    except Exception as e:
        logger.error(f"Failed to initialize mappings: {e}")
        raise

@app.post("/start-process")
async def start_process(background_tasks: BackgroundTasks):
    """
    Start processing emails asynchronously and immediately return process_id.
    """
    try:
        process_id = str(uuid4())
        background_tasks.add_task(process_all_emails, process_id, process_status)
        logger.info(f"Started process {process_id}")
        return {"process_id": process_id}
    except Exception as e:
        logger.error(f"Error starting process: {e}")
        raise HTTPException(status_code=500, detail="Failed to start process")

@app.get("/status/{process_id}")
async def get_status(process_id: str):
    """
    Get current status of a background parsing task.
    """
    status = load_process_status(process_id)
    if not status:
        logger.warning(f"Status not found for process {process_id}")
        raise HTTPException(status_code=404, detail="Process not found")
    return status

@app.get("/download/{filename}")
async def download_file(filename: str):
    """
    Download output CSV or failed CSV by filename.
    """
    file_path = Path("output") / filename
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
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