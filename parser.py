import os
import re
import imaplib
import aiohttp
import asyncio
import logging
import pandas as pd
import json
import csv
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from email import policy
from email.parser import BytesParser

# --- Config ---
BASE_DIR: Path = Path(__file__).parent  # Define BASE_DIR at the start

OUTPUT_DIR: Path = BASE_DIR / "output"
PROMPT_DIR: Path = BASE_DIR / "prompts"
SUPPLIER_PROMPT_FILE = PROMPT_DIR / "supplier_chat_prompt.txt"
OPIS_PROMPT_FILE = PROMPT_DIR / "opis_chat_prompt.txt"

IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
EMAIL_ACCOUNT = os.getenv("IMAP_USERNAME")  # Use your existing IMAP_USERNAME
EMAIL_PASSWORD = os.getenv("IMAP_PASSWORD")  # Use your existing IMAP_PASSWORD
GROK_API_KEY = os.getenv("XAI_API_KEY")  # Use your existing XAI_API_KEY

# API endpoint and model for Grok
GROK_API_URL = "https://api.x.ai/v1/chat/completions"
GROK_MODEL = "grok-3-latest"

# --- Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

process_status = {}  # Track background process states

# Configure logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("parser")

# Output and Prompts directory
BASE_DIR: Path = BASE_DIR
OUTPUT_DIR: Path = BASE_DIR / "output"
PROMPTS_DIR: Path = BASE_DIR / "prompts"
STATE_DIR: Path = BASE_DIR / "state"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Verify STATE_DIR permissions
logger.info(f"STATE_DIR path: {STATE_DIR}")
if not STATE_DIR.exists():
    logger.error("STATE_DIR does not exist after mkdir")
if not os.access(STATE_DIR, os.W_OK):
    logger.error("STATE_DIR is not writable")

# Global mappings
SUPPLIER_MAPPING: Dict[str, str] = {}
PRODUCT_MAPPING: Dict[str, str] = {}
TERMINAL_MAPPING: List[Dict[str, str]] = []

# Expected price range for validation
EXPECTED_PRICE_RANGE = {"E10": (1.50, 4.00), "ULSD": (1.80, 3.50)}
PRICE_TOLERANCE = 0.01
REQUIRED_PRODUCTS = ["ULSD", "87E10"]

# --- State Persistence Functions ---

def save_process_status(process_id: str, status: Dict) -> None:
    """Save the process status to a file."""
    try:
        state_file = STATE_DIR / f"process_{process_id}.json"
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(status, f)
        logger.debug(f"Saved status for process {process_id} to {state_file}")
    except Exception as e:
        logger.error(f"Failed to save status for process {process_id}: {e}")
        raise

def load_process_status(process_id: str) -> Optional[Dict]:
    """Load the process status from a file."""
    state_file = STATE_DIR / f"process_{process_id}.json"
    if state_file.exists():
        with open(state_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def delete_process_status(process_id: str) -> None:
    """Delete the process status file."""
    state_file = STATE_DIR / f"process_{process_id}.json"
    if state_file.exists():
        state_file.unlink()
        logger.info(f"Deleted status file for process {process_id}")

# --- Utilities ---

def load_env() -> Dict[str, str]:
    load_dotenv()
    env_vars = {
        "IMAP_SERVER": os.getenv("IMAP_SERVER", "imap.gmail.com"),
        "IMAP_USERNAME": os.getenv("IMAP_USERNAME", ""),
        "IMAP_PASSWORD": os.getenv("IMAP_PASSWORD", ""),
        "XAI_API_KEY": os.getenv("XAI_API_KEY", ""),
        "MODEL": os.getenv("MODEL", "grok-3-latest"),
    }
    for key, value in env_vars.items():
        if not value:
            raise ValueError(f"Missing environment variable: {key}")
    return env_vars

def initialize_mappings() -> None:
    global SUPPLIER_MAPPING, PRODUCT_MAPPING, TERMINAL_MAPPING
    mappings_file = BASE_DIR / "mappings.xlsx"
    if not mappings_file.exists():
        raise FileNotFoundError(f"Mappings file not found: {mappings_file}")

    df_suppliers = pd.read_excel(mappings_file, sheet_name="SupplierMappings")
    SUPPLIER_MAPPING = dict(zip(df_suppliers["Raw Value"], df_suppliers["Standardized Value"]))

    df_products = pd.read_excel(mappings_file, sheet_name="ProductMappings")
    PRODUCT_MAPPING = dict(zip(df_products["Raw Value"], df_products["Standardized Value"]))

    df_terminals = pd.read_excel(mappings_file, sheet_name="TerminalMappings")
    TERMINAL_MAPPING = []
    for _, row in df_terminals.iterrows():
        TERMINAL_MAPPING.append({
            "raw_value": str(row["Raw Value"]),
            "standardized_value": str(row["Standardized Value"]),
            "condition": str(row.get("Condition", "")) if pd.notna(row.get("Condition")) else None
        })

# --- Email Handling ---

def choose_best_content_from_email(msg) -> str:
    for part in msg.walk():
        filename = part.get_filename()
        if filename and filename.endswith(".txt"):
            try:
                content = part.get_payload(decode=True).decode(errors="ignore")
                if content.strip():
                    logger.info(f"Using .txt attachment: {filename}")
                    return content
            except Exception as e:
                logger.error(f"Failed to decode attachment {filename}: {e}")
    body = msg.get_body(preferencelist=("plain"))
    if body:
        return body.get_content().strip()
    return ""

def clean_email_content(content: str) -> str:
    try:
        content = content.replace("=\n", "").replace("=20", " ")
        content = re.sub(r"-{40,}", "", content)
        content = re.sub(r"\n{3,}", "\n\n", content)
        cleaned = "\n".join(line.strip() for line in content.splitlines())
        logger.debug(f"Cleaned content length: {len(cleaned)}")
        return cleaned  # Removed [:6000] limit
    except Exception as e:
        logger.error(f"Failed to clean content: {e}")
        return content

# --- IMAP Functions ---

def fetch_emails(env: Dict[str, str], process_id: str) -> List[Dict[str, str]]:
    emails = []
    try:
        imap_server = imaplib.IMAP4_SSL(env["IMAP_SERVER"])
        imap_server.login(env["IMAP_USERNAME"], env["IMAP_PASSWORD"])
        imap_server.select("INBOX")
        since_date = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
        _, msg_nums = imap_server.search(None, f'(SINCE "{since_date}") UNSEEN')
        for num in msg_nums[0].split():
            _, data = imap_server.fetch(num, "(RFC822)")
            msg = BytesParser(policy=policy.default).parsebytes(data[0][1])
            content = choose_best_content_from_email(msg)
            subject = msg.get("Subject", "").strip()
            from_addr = msg.get("From", "").strip()
            if content:
                emails.append({
                    "uid": num.decode(),
                    "content": content,
                    "subject": subject,
                    "from_addr": from_addr,
                })
        imap_server.logout()
    except Exception as e:
        logger.error(f"Failed to fetch emails: {e}")
    return emails

def mark_email_as_processed(uid: str, env: Dict[str, str]) -> None:
    try:
        imap_server = imaplib.IMAP4_SSL(env["IMAP_SERVER"])
        imap_server.login(env["IMAP_USERNAME"], env["IMAP_PASSWORD"])
        imap_server.select("INBOX")
        imap_server.store(uid, '+X-GM-LABELS', 'BDE_Processed')
        imap_server.logout()
        logger.info(f"Marked email {uid} as processed")
    except Exception as e:
        logger.error(f"Failed to mark processed: {e}")

# --- Grok API Functions ---

def load_prompt(filename: str) -> str:
    prompt_path = PROMPT_DIR / filename
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

async def call_grok_api(prompt: str, content: str, env: Dict[str, str], session: aiohttp.ClientSession) -> Optional[str]:
    try:
        api_url = "https://api.x.ai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {env['XAI_API_KEY']}", "Content-Type": "application/json"}
        payload = {
            "model": env.get("MODEL", "grok-3-latest"),
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": content},
            ]
        }
        async with session.post(api_url, headers=headers, json=payload) as response:
            response.raise_for_status()
            raw_text = await response.text()
        data = json.loads(raw_text)
        return data.get("choices", [{}])[0].get("message", {}).get("content", "[]")
    except Exception as e:
        logger.error(f"Grok API call failed: {e}")
        return None

# --- Processing Functions ---

async def process_email_with_delay(email: Dict[str, str], env: Dict[str, str], process_id: str, session: aiohttp.ClientSession) -> Tuple[List[Dict], List[Dict], Optional[Dict]]:
    valid_rows, skipped_rows, failed_email = [], [], None
    try:
        content = clean_email_content(email.get("content", ""))
        if not content:
            raise ValueError("Empty email content")

        is_opis = "OPIS" in content and ("Rack" in content or "Wholesale" in content) and "Effective Date" in content
        logger.debug(f"Email UID {email.get('uid', '?')} classified as {'OPIS' if is_opis else 'Supplier'}")
        prompt_file = "opis_chat_prompt.txt" if is_opis else "supplier_chat_prompt.txt"
        prompt_chat = load_prompt(prompt_file)

        parsed = await call_grok_api(prompt_chat, content, env, session)
        logger.debug(f"Grok raw response: {parsed[:1000]}...")  # Log first 1000 chars
        if parsed.startswith("```json"):
            match = re.search(r"```json\s*(.*?)\s*```", parsed, re.DOTALL)
            if match:
                parsed = match.group(1).strip()

        rows = json.loads(parsed)
        logger.debug(f"Parsed {len(rows)} rows from email UID {email.get('uid', '?')}")
        for row in rows:
            valid_rows.append({
                "Supplier": row.get("Supplier", ""),
                "Supply": row.get("Supply", ""),
                "Product Name": row.get("Product Name", ""),
                "Terminal": row.get("Terminal", ""),
                "Price": row.get("Price", 0),
                "Volume Type": row.get("Volume Type", ""),
                "Effective Date": row.get("Effective Date", ""),
                "Effective Time": row.get("Effective Time", ""),
            })

        if valid_rows:
            mark_email_as_processed(email.get("uid", ""), env)

    except Exception as ex:
        failed_email = {"email_id": email.get("uid", "?"), "subject": email.get("subject", ""), "from_addr": email.get("from_addr", ""), "error": str(ex)}
        logger.error(f"Failed to process email UID {email.get('uid', '?')}: {str(ex)}")
    return valid_rows, skipped_rows, failed_email

async def process_all_emails(process_id: str, process_statuses: Dict[str, dict]) -> None:
    try:
        env = load_env()
        # Initialize and save status file immediately
        initial_status = {
            "status": "running",
            "email_count": 0,
            "current_email": 0,
            "row_count": 0,
            "output_file": None,
            "error": None,
        }
        process_statuses[process_id] = initial_status
        save_process_status(process_id, initial_status)
        logger.info(f"Started process {process_id}, saved initial status")

        emails = fetch_emails(env, process_id)
        process_statuses[process_id]["email_count"] = len(emails)
        save_process_status(process_id, process_statuses[process_id])
        logger.info(f"Fetched {len(emails)} emails for process {process_id}")

        if not emails:
            process_statuses[process_id]["status"] = "done"
            save_process_status(process_id, process_statuses[process_id])
            logger.info(f"No emails to process for {process_id}")
            return

        output_file = f"parsed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        total_rows = 0
        failed_emails = []

        async with aiohttp.ClientSession() as session:
            for idx, email in enumerate(emails):
                process_statuses[process_id]["current_email"] = idx + 1
                save_process_status(process_id, process_statuses[process_id])
                logger.info(f"Processing email {idx + 1}/{len(emails)} for process {process_id}")
                valid_rows, skipped_rows, failed_email = await process_email_with_delay(email, env, process_id, session)

                if valid_rows:
                    save_to_csv(valid_rows, output_file, process_id)
                    total_rows += len(valid_rows)
                    process_statuses[process_id]["row_count"] = total_rows
                    save_process_status(process_id, process_statuses[process_id])

                if failed_email:
                    failed_email["content"] = email.get("content", "")
                    failed_emails.append(failed_email)

                await asyncio.sleep(2)

        if failed_emails:
            save_failed_emails_to_csv(failed_emails, output_file, process_id)

        process_statuses[process_id]["status"] = "done"
        process_statuses[process_id]["output_file"] = output_file
        save_process_status(process_id, process_statuses[process_id])
        logger.info(f"Completed process {process_id} with {total_rows} rows")
    except Exception as e:
        logger.error(f"Error in process_all_emails for process {process_id}: {str(e)}")
        process_statuses[process_id]["status"] = "error"
        process_statuses[process_id]["error"] = str(e)
        save_process_status(process_id, process_statuses[process_id])

# --- CSV Saving Functions ---

def save_to_csv(data: List[Dict], output_filename: str, process_id: str) -> None:
    try:
        output_path = OUTPUT_DIR / output_filename
        fieldnames = ["Supplier", "Supply", "Product Name", "Terminal", "Price", "Volume Type", "Effective Date", "Effective Time"]
        mode = "a" if output_path.exists() else "w"
        with open(output_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if mode == "w":
                writer.writeheader()
            for row in data:
                writer.writerow(row)
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")

def save_failed_emails_to_csv(failed_emails: List[Dict], output_filename: str, process_id: str) -> None:
    try:
        if failed_emails:
            failed_filename = f"failed_{output_filename}"
            failed_path = OUTPUT_DIR / failed_filename
            df_failed = pd.DataFrame(failed_emails)
            df_failed.to_csv(failed_path, index=False)
    except Exception as e:
        logger.error(f"Failed to save failed emails: {e}")