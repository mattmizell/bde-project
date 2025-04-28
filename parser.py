# parser.py
import imaplib
import email
from email.header import decode_header
import os
import requests
import pandas as pd
import re
from datetime import datetime
import json
from pathlib import Path
import logging

# --- Config ---
IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
EMAIL_ACCOUNT = os.getenv("IMAP_USERNAME")  # Use your existing IMAP_USERNAME
EMAIL_PASSWORD = os.getenv("IMAP_PASSWORD")  # Use your existing IMAP_PASSWORD
GROK_API_KEY = os.getenv("XAI_API_KEY")  # Use your existing XAI_API_KEY
OUTPUT_DIR = Path("output")
PROMPT_DIR = Path("prompts")
SUPPLIER_PROMPT_FILE = PROMPT_DIR / "supplier_chat_prompt.txt"
OPIS_PROMPT_FILE = PROMPT_DIR / "opis_chat_prompt.txt"

# API endpoint and model for Grok
GROK_API_URL = "https://api.x.ai/v1/chat/completions"
GROK_MODEL = "grok-3-latest"

# --- Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

process_status = {}  # Track background process states

# --- Utilities ---

def initialize_mappings():
    """Placeholder for future supplier/product mappings."""
    logger.info("Mappings initialized (placeholder)")

def load_process_status(process_id):
    return process_status.get(process_id)

def delete_process_status(process_id):
    if process_id in process_status:
        del process_status[process_id]

def clean_email_content(content):
    """General email body cleaner."""
    content = content.replace("\r\n", "\n").replace("\r", "\n")
    content = re.sub(r"[ \t]+", " ", content)
    content = re.sub(r"\n{2,}", "\n", content)
    return content.strip()

def clean_opis_content(content):
    """Specific cleaner for OPIS reports."""
    content = re.sub(
        r"(?i)^\s*(LOW RACK|HIGH RACK|RACK AVG|BRD LOW RACK|BRD HIGH RACK|BRD RACK AVG|SPOT MEAN|FOB COLONIAL|FOB ST\. LOUIS|CONT AVG.*|CONT LOW.*|CONT HIGH.*|ADDITIONAL CONTRACT SUMMARY.*|UBD.*|BRD.*|ST\. LOUIS, MO-IL.*)$",
        "",
        content,
        flags=re.MULTILINE
    )
    content = re.sub(r"[ ]{2,}", " ", content)
    content = re.sub(r"\n{2,}", "\n", content)
    return content.strip()

def call_grok_api(content, prompt_file):
    """Call Grok 3 API manually."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GROK_API_KEY}"
    }

    with open(prompt_file, "r") as f:
        prompt = f.read()

    body = {
        "model": GROK_MODEL,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": content}
        ],
        "temperature": 0,
        "max_tokens": 4096
    }

    response = requests.post(GROK_API_URL, headers=headers, json=body)

    if response.status_code != 200:
        raise Exception(f"Grok API Error: {response.status_code}: {response.text}")

    reply = response.json()['choices'][0]['message']['content']
    return json.loads(reply)

# --- Main Email Processing ---

def process_all_emails(process_id):
    logger.info(f"Started processing emails with process_id={process_id}")
    process_status[process_id] = {
        "status": "processing",
        "email_count": 0,
        "current_email": 0,
        "row_count": 0,
        "output_file": None
    }

    output_rows = []
    failed_emails = []

    OUTPUT_DIR.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"parsed_{today}.csv"
    failed_file = f"failed_{today}.csv"

    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
        mail.select("inbox")

        result, data = mail.search(None, "ALL")
        email_ids = data[0].split()
        process_status[process_id]["email_count"] = len(email_ids)

        for idx, email_id in enumerate(email_ids):
            process_status[process_id]["current_email"] = idx + 1

            result, msg_data = mail.fetch(email_id, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            subject, encoding = decode_header(msg["Subject"])[0]
            if isinstance(subject, bytes):
                subject = subject.decode(encoding or "utf-8")

            logger.info(f"Processing email: {subject}")

            # Prefer .txt attachments
            email_content = None
            for part in msg.walk():
                if part.get_filename() and part.get_filename().endswith(".txt"):
                    email_content = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                    break

            if not email_content:
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            email_content = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                            break
                else:
                    email_content = msg.get_payload(decode=True).decode("utf-8", errors="ignore")

            if not email_content:
                logger.warning("No content found in email.")
                continue

            clean_text = clean_email_content(email_content)

            if "opis" in subject.lower():
                clean_text = clean_opis_content(clean_text)
                prompt_file = OPIS_PROMPT_FILE
            else:
                prompt_file = SUPPLIER_PROMPT_FILE

            try:
                extracted_rows = call_grok_api(clean_text, prompt_file)
                output_rows.extend(extracted_rows)
                process_status[process_id]["row_count"] += len(extracted_rows)
            except Exception as e:
                logger.error(f"Failed to parse email {subject}: {str(e)}")
                failed_emails.append(subject)

        # Save parsed rows
        if output_rows:
            pd.DataFrame(output_rows).to_csv(OUTPUT_DIR / output_file, index=False)
            process_status[process_id]["output_file"] = output_file

        # Save failed emails
        if failed_emails:
            with open(OUTPUT_DIR / failed_file, "w") as f:
                for subj in failed_emails:
                    f.write(f"{subj}\n")

        process_status[process_id]["status"] = "done"

    except Exception as e:
        logger.exception(f"Critical error in processing: {str(e)}")
        process_status[process_id]["status"] = "error"
        process_status[process_id]["error"] = str(e)
