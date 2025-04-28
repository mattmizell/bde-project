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
BASE_DIR: Path = Path(__file__).parent

OUTPUT_DIR: Path = BASE_DIR / "output"
PROMPT_DIR: Path = BASE_DIR / "prompts"
SUPPLIER_PROMPT_FILE = PROMPT_DIR / "supplier_chat_prompt.txt"
OPIS_PROMPT_FILE = PROMPT_DIR / "opis_chat_prompt.txt"

IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
EMAIL_ACCOUNT = os.getenv("IMAP_USERNAME")
EMAIL_PASSWORD = os.getenv("IMAP_PASSWORD")
GROK_API_KEY = os.getenv("XAI_API_KEY")

GROK_API_URL = "https://api.x.ai/v1/chat/completions"
GROK_MODEL = "grok-3-latest"

# --- Setup ---
process_status = {}

# Configure logger
logger = logging.getLogger("parser")
logger.setLevel(logging.DEBUG)

# Remove any existing handlers to avoid conflicts
logger.handlers = []

# Add a console handler for Render logs
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Output, Prompts, and State directories
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PROMPT_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR: Path = BASE_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Verify STATE_DIR permissions
logger.info(f"STATE_DIR path: {STATE_DIR}")
if not STATE_DIR.exists():
    logger.error("STATE_DIR does not exist after mkdir")
if not os.access(STATE_DIR, os.W_OK):
    logger.error("STATE_DIR is not writable")

# --- File Logging Setup ---
def setup_file_logging(process_id: str) -> logging.Handler:
    log_file = OUTPUT_DIR / f"debug_{process_id}.txt"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.debug(f"File logging set up for process {process_id} at level {logging.getLevelName(logger.level)}")
    logger.debug(f"Logging to file: {log_file}")
    return file_handler

def remove_file_logging(handler: logging.Handler) -> None:
    logger.removeHandler(handler)
    handler.close()
    logger.debug("File logging handler removed")

# --- State Persistence Functions ---
def save_process_status(process_id: str, status: Dict) -> None:
    try:
        state_file = STATE_DIR / f"process_{process_id}.json"
        state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(status, f)
        logger.debug(f"Saved status for process {process_id} to {state_file}")
    except Exception as e:
        logger.error(f"Failed to save status for process {process_id}: {e}")
        raise

def load_process_status(process_id: str) -> Optional[Dict]:
    state_file = STATE_DIR / f"process_{process_id}.json"
    try:
        if state_file.exists():
            with open(state_file, "r", encoding="utf-8") as f:
                status = json.load(f)
                logger.debug(f"Loaded status for process {process_id}: {status}")
                return status
        else:
            logger.warning(f"Status file not found for process {process_id}: {state_file}")
            return None
    except Exception as e:
        logger.error(f"Failed to load status for process {process_id}: {e}")
        return None

def delete_process_status(process_id: str) -> None:
    state_file = STATE_DIR / f"process_{process_id}.json"
    try:
        if state_file.exists():
            state_file.unlink()
            logger.info(f"Deleted status file for process {process_id}")
        else:
            logger.warning(f"Status file to delete not found for process {process_id}: {state_file}")
    except Exception as e:
        logger.error(f"Failed to delete status file for process {process_id}: {e}")

# --- Load Translation Mappings ---
def load_mappings(file_path: str = "mappings.xlsx") -> Dict[str, Dict]:
    try:
        xl = pd.ExcelFile(file_path)
        mappings = {"suppliers": {}, "products": {}, "terminals": {}}

        logger.debug(f"Available sheets in {file_path}: {xl.sheet_names}")

        supplier_sheet = None
        for sheet_name in ["SupplierMappings", "Suppliers", "Supplier Mappings"]:
            if sheet_name in xl.sheet_names:
                supplier_sheet = sheet_name
                break
        if supplier_sheet:
            df_suppliers = xl.parse(supplier_sheet)
            for _, row in df_suppliers.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                mappings["suppliers"][raw_value] = standardized_value
            logger.debug(f"Loaded {len(mappings['suppliers'])} supplier mappings from sheet '{supplier_sheet}'")
        else:
            logger.warning("Supplier mappings sheet not found. Expected 'SupplierMappings', 'Suppliers', or 'Supplier Mappings'.")

        product_sheet = None
        for sheet_name in ["ProductMappings", "Products", "Product Mappings"]:
            if sheet_name in xl.sheet_names:
                product_sheet = sheet_name
                break
        if product_sheet:
            df_products = xl.parse(product_sheet)
            for _, row in df_products.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                mappings["products"][raw_value] = standardized_value
            logger.debug(f"Loaded {len(mappings['products'])} product mappings from sheet '{product_sheet}'")
        else:
            logger.warning("Product mappings sheet not found. Expected 'ProductMappings', 'Products', or 'Product Mappings'.")

        terminal_sheet = None
        for sheet_name in ["TerminalMappings", "Terminals", "Terminal Mappings"]:
            if sheet_name in xl.sheet_names:
                terminal_sheet = sheet_name
                break
        if terminal_sheet:
            df_terminals = xl.parse(terminal_sheet)
            for _, row in df_terminals.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                condition = row.get("Condition", None)
                if raw_value not in mappings["terminals"]:
                    mappings["terminals"][raw_value] = []
                mappings["terminals"][raw_value].append({
                    "standardized": standardized_value,
                    "condition": condition if pd.notna(condition) else None
                })
            logger.debug(f"Loaded {len(mappings['terminals'])} terminal mappings from sheet '{terminal_sheet}'")
        else:
            logger.warning("Terminal mappings sheet not found. Expected 'TerminalMappings', 'Terminals', or 'Terminal Mappings'.")

        logger.debug(f"Total mappings loaded: Suppliers={len(mappings['suppliers'])}, Products={len(mappings['products'])}, Terminals={len(mappings['terminals'])}")
        return mappings
    except Exception as e:
        logger.error(f"Failed to load mappings from {file_path}: {e}")
        return {"suppliers": {}, "products": {}, "terminals": {}}

# --- Extract Position Holder from Terminal ---
def extract_position_holder(terminal: str) -> str:
    # Common position holders
    position_holders = {
        "FH": "Flint Hills",
        "GMK": "Growmark",
        "SC": "Sinclair",
        "VL": "Valero",
        "JDS": "JDS",
        "MG": "Magellan",
        "HEP": "Holly Energy Partners",
        "STL": "St. Louis",
        "KMEP": "Kinder Morgan Energy Partners",
        "PSX": "Phillips 66",
        "Marathon": "Marathon"
    }
    # Split terminal by common separators: -, /, commas, and spaces
    parts = re.split(r'[-/,]|\s+', terminal.strip())
    # Remove empty parts and normalize
    parts = [part.strip() for part in parts if part.strip()]

    # Check each part against known position holders
    for part in parts:
        if part in position_holders:
            return position_holders[part]
        # Also check for prefixes (e.g., "MG" in "MG-BETTENDORF")
        for prefix, holder in position_holders.items():
            if part.startswith(prefix) and prefix != "Marathon":  # Marathon is a full match, not a prefix
                return holder

    # Fallback: Use the last part if no match (e.g., "Marathon" at the end)
    return parts[-1] if parts else terminal

# --- Apply Mappings to Rows ---
def apply_mappings(row: Dict, mappings: Dict[str, Dict], is_opis: bool, email_from: str) -> Dict:
    # Supplier: For non-OPIS emails, use the email sender; for OPIS, use the parsed supplier
    if not is_opis:
        # Extract supplier from email "From" field (e.g., "Luke Oil Company <email@domain.com>")
        supplier_match = re.search(r'^(.*?)(?:\s*<|$)', email_from)
        supplier = supplier_match.group(1).strip() if supplier_match else email_from
        row["Supplier"] = supplier
    else:
        supplier = row.get("Supplier", "")

    # Apply supplier mapping
    if supplier in mappings["suppliers"]:
        row["Supplier"] = mappings["suppliers"][supplier]
        logger.debug(f"Translated Supplier: {supplier} -> {row['Supplier']}")

    # Supply: Extract position holder from terminal
    terminal = row.get("Terminal", "")
    supply = extract_position_holder(terminal)
    # Apply supplier mapping to supply (position holder)
    if supply in mappings["suppliers"]:
        supply = mappings["suppliers"][supply]
        logger.debug(f"Translated Supply: {supply} -> {mappings['suppliers'][supply]}")
    row["Supply"] = supply

    # Product mappings
    product = row.get("Product Name", "")
    product_key = product.replace("Gross ", "") if product.startswith("Gross ") else product
    if product_key in mappings["products"]:
        row["Product Name"] = mappings["products"][product_key]
        logger.debug(f"Translated Product: {product} -> {row['Product Name']}")
    else:
        product_key = product_key.replace("Wholesale ", "")
        if product_key in mappings["products"]:
            row["Product Name"] = mappings["products"][product_key]
            logger.debug(f"Translated Product: {product} -> {row['Product Name']}")

    # Terminal mappings
    supplier = row.get("Supplier", "")
    if terminal in mappings["terminals"]:
        for mapping in mappings["terminals"][terminal]:
            condition = mapping["condition"]
            if condition is None:
                row["Terminal"] = mapping["standardized"]
                logger.debug(f"Translated Terminal: {terminal} -> {row['Terminal']}")
                break
            elif condition == 'Supplier in ["Phillips 66", "Cenex"]' and supplier in ["Phillips 66", "Cenex"]:
                row["Terminal"] = mapping["standardized"]
                logger.debug(f"Translated Terminal: {terminal} -> {row['Terminal']} (condition: Supplier in ['Phillips 66', 'Cenex'])")
                break
            elif condition == 'Supplier not in ["Phillips 66", "Cenex"]' and supplier not in ["Phillips 66", "Cenex"]:
                row["Terminal"] = mapping["standardized"]
                logger.debug(f"Translated Terminal: {terminal} -> {row['Terminal']} (condition: Supplier not in ['Phillips 66', 'Cenex'])")
                break

    return row

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
        return cleaned
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

async def call_grok_api(prompt: str, content: str, env: Dict[str, str], session: aiohttp.ClientSession, process_id: str) -> Optional[str]:
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
            data = await response.json()
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

        mappings = load_mappings("mappings.xlsx")

        content_lower = content.lower()
        subject_lower = email.get("subject", "").lower()
        is_opis = ("opis" in content_lower and ("rack" in content_lower or "wholesale" in content_lower)) or \
                  ("opis" in subject_lower and ("rack" in subject_lower or "wholesale" in subject_lower))
        logger.debug(f"Email UID {email.get('uid', '?')} classified as {'OPIS' if is_opis else 'Supplier'}")
        logger.debug(f"Email subject: {email.get('subject', '')}")
        logger.debug(f"Email content (first 500 chars): {content[:500]}")
        prompt_file = "opis_chat_prompt.txt" if is_opis else "supplier_chat_prompt.txt"
        prompt_chat = load_prompt(prompt_file)

        parsed = await call_grok_api(prompt_chat, content, env, session, process_id)
        logger.debug(f"Grok raw response for UID {email.get('uid', '?')}: {parsed}")
        if parsed.startswith("```json"):
            match = re.search(r"```json\s*(.*?)\s*```", parsed, re.DOTALL)
            if match:
                parsed = match.group(1).strip()

        rows = json.loads(parsed)
        logger.debug(f"Parsed {len(rows)} rows from email UID {email.get('uid', '?')}: {rows}")
        for row in rows:
            if not row.get("Product Name") or not row.get("Terminal") or not isinstance(row.get("Price"), (int, float)):
                logger.debug(f"Skipping row due to missing required fields: {row}")
                continue
            price = row.get("Price", 0)
            if is_opis and price > 10:
                price = price / 100
                row["Price"] = price
                logger.debug(f"Converted price from cents to dollars: {row}")
            if price > 10:
                logger.debug(f"Skipping row due to price > 10: {row}")
                continue
            row = apply_mappings(row, mappings, is_opis, email.get("from_addr", ""))
            valid_rows.append({
                "Supplier": row.get("Supplier", ""),
                "Supply": row.get("Supply", ""),
                "Product Name": row.get("Product Name", ""),
                "Terminal": row.get("Terminal", ""),
                "Price": price,
                "Volume Type": row.get("Volume Type", ""),
                "Effective Date": row.get("Effective Date", ""),
                "Effective Time": row.get("Effective Time", ""),
            })

        if valid_rows:
            mark_email_as_processed(email.get("uid", ""), env)
            logger.info(f"Successfully parsed {len(valid_rows)} valid rows from email UID {email.get('uid', '?')}")

    except Exception as ex:
        failed_email = {"email_id": email.get("uid", "?"), "subject": email.get("subject", ""), "from_addr": email.get("from_addr", ""), "error": str(ex)}
        logger.error(f"Failed to process email UID {email.get('uid', '?')}: {str(ex)}")
    return valid_rows, skipped_rows, failed_email

async def process_all_emails(process_id: str, process_statuses: Dict[str, dict]) -> None:
    file_handler = setup_file_logging(process_id)
    try:
        env = load_env()
        initial_status = {
            "status": "running",
            "email_count": 0,
            "current_email": 0,
            "row_count": 0,
            "output_file": None,
            "error": None,
            "debug_log": f"debug_{process_id}.txt",
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

        await asyncio.sleep(60)
        delete_process_status(process_id)
    except Exception as e:
        logger.error(f"Error in process_all_emails for process {process_id}: {str(e)}")
        process_statuses[process_id]["status"] = "error"
        process_statuses[process_id]["error"] = str(e)
        save_process_status(process_id, process_statuses[process_id])
    finally:
        remove_file_logging(file_handler)

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