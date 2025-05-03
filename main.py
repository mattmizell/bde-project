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
    save_process_status,  # ✅ Add this line
    logger,
    load_env
)

#touch
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://bde-frontend-pf3m.onrender.com",
        "https://bde-frontend-dropdown-test.onrender.com",  # ✅ Add this
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
import sys

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Named logger for this module
logger = logging.getLogger("main")



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
# trigger redeploy
