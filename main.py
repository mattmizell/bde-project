import uuid
import logging
import aiofiles
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from parser import (
    process_all_emails,
    process_status,
    load_process_status,
    delete_process_status,
    logger,
    load_env
)

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://bde-frontend-pf3m.onrender.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logger.setLevel(logging.INFO)

@app.post("/start-process")
async def start_process(request: Request, background_tasks: BackgroundTasks):
    process_id = str(uuid.uuid4())
    model = request.query_params.get("model", "grok-3")
    logger.info(f"🚀 Received start-process request. Assigned process_id={process_id}, model={model}")

    try:
        env = load_env()
        logger.info(f"🌎 Loaded environment from .env: {env}")
        env["MODEL"] = model  # Override with selected model
        logger.info(f"🧠 Overriding MODEL for this run to: {model}")
    except Exception as e:
        logger.error(f"❌ Failed to load or override environment variables: {e}")
        raise HTTPException(status_code=500, detail="Environment loading failed")

    process_status[process_id] = {
        "status": "starting",
        "email_count": 0,
        "current_email": 0,
        "row_count": 0,
        "output_file": None,
        "error": None,
        "debug_log": f"debug_{process_id}.txt"
    }

    logger.info(f"📦 Initialized process status and starting background task for process_id={process_id}")
    background_tasks.add_task(process_all_emails, process_id, process_status, env)
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
            await f.read()  # Verify existence
        return FileResponse(file_path, media_type='application/octet-stream', filename=filename)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="The file was not found")

@app.post("/cleanup/{process_id}")
async def cleanup_process(process_id: str):
    delete_process_status(process_id)
    return {"message": "Process status deleted"}
