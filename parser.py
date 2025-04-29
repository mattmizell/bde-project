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
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Prevent propagation to root logger to avoid duplicate logs
logger.propagate = False

# Set other loggers to INFO level to reduce noise
logging.getLogger("aiohttp").setLevel(logging.INFO)
logging.getLogger("asyncio").setLevel(logging.INFO)

# As a fallback, add a DEBUG-level console handler to the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
    root_console_handler = logging.StreamHandler()
    root_console_handler.setLevel(logging.DEBUG)
    root_console_handler.setFormatter(formatter)
    root_logger.addHandler(root_console_handler)

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
    logger.debug(f"Entering load_mappings with file_path: {file_path}")
    try:
        xl = pd.ExcelFile(file_path)
        mappings = {"suppliers": {}, "domain_to_supplier": {}, "position_holders": {}, "products": {}, "terminals": {}}
        logger.debug(f"Excel file loaded, available sheets: {xl.sheet_names}")

        # Load SupplierMappings
        supplier_sheet = None
        for sheet_name in ["SupplierMappings", "Suppliers", "Supplier Mappings"]:
            if sheet_name in xl.sheet_names:
                supplier_sheet = sheet_name
                break
        if supplier_sheet:
            logger.debug(f"Loading SupplierMappings from sheet: {supplier_sheet}")
            df_suppliers = xl.parse(supplier_sheet)
            logger.debug(f"SupplierMappings dataframe loaded with shape: {df_suppliers.shape}")
            for index, row in df_suppliers.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                domain = str(row.get("Domain", "")).strip()  # ✅ STRIP FIXED HERE
                logger.debug(f"Processing SupplierMappings row {index}: Raw Value={raw_value}, Standardized Value={standardized_value}, Domain={domain}")
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in SupplierMappings: {row.to_dict()}")
                    continue
                mappings["suppliers"][raw_value] = standardized_value
                if domain:
                    mappings["domain_to_supplier"][domain.lower()] = standardized_value
                    logger.debug(f"Added domain mapping: {domain.lower()} -> {standardized_value}")
            logger.debug(f"Loaded {len(mappings['suppliers'])} supplier mappings from sheet '{supplier_sheet}'")
            logger.debug(f"Loaded {len(mappings['domain_to_supplier'])} domain-to-supplier mappings: {mappings['domain_to_supplier']}")
        else:
            logger.warning("Supplier mappings sheet not found. Expected 'SupplierMappings', 'Suppliers', or 'Supplier Mappings'.")

        # Load SupplyMappings
        supply_sheet = None
        for sheet_name in ["SupplyMappings", "Supply", "Supply Mappings"]:
            if sheet_name in xl.sheet_names:
                supply_sheet = sheet_name
                break
        if supply_sheet:
            logger.debug(f"Loading SupplyMappings from sheet: {supply_sheet}")
            df_supply = xl.parse(supply_sheet)
            logger.debug(f"SupplyMappings dataframe loaded with shape: {df_supply.shape}")
            for index, row in df_supply.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                logger.debug(f"Processing SupplyMappings row {index}: Raw Value={raw_value}, Standardized Value={standardized_value}")
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in SupplyMappings: {row.to_dict()}")
                    continue
                mappings["position_holders"][raw_value] = standardized_value
            logger.debug(f"Loaded {len(mappings['position_holders'])} position holder mappings from sheet '{supply_sheet}': {mappings['position_holders']}")
        else:
            logger.warning("Supply mappings sheet not found. Expected 'SupplyMappings', 'Supply', or 'Supply Mappings'.")

        # Load ProductMappings
        product_sheet = None
        for sheet_name in ["ProductMappings", "Products", "Product Mappings"]:
            if sheet_name in xl.sheet_names:
                product_sheet = sheet_name
                break
        if product_sheet:
            logger.debug(f"Loading ProductMappings from sheet: {product_sheet}")
            df_products = xl.parse(product_sheet)
            logger.debug(f"ProductMappings dataframe loaded with shape: {df_products.shape}")
            for index, row in df_products.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                logger.debug(f"Processing ProductMappings row {index}: Raw Value={raw_value}, Standardized Value={standardized_value}")
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in ProductMappings: {row.to_dict()}")
                    continue
                mappings["products"][raw_value] = standardized_value
            logger.debug(f"Loaded {len(mappings['products'])} product mappings from sheet '{product_sheet}': {mappings['products']}")
        else:
            logger.warning("Product mappings sheet not found. Expected 'ProductMappings', 'Products', or 'Product Mappings'.")

        # Load TerminalMappings
        terminal_sheet = None
        for sheet_name in ["TerminalMappings", "Terminals", "Terminal Mappings"]:
            if sheet_name in xl.sheet_names:
                terminal_sheet = sheet_name
                break
        if terminal_sheet:
            logger.debug(f"Loading TerminalMappings from sheet: {terminal_sheet}")
            df_terminals = xl.parse(terminal_sheet)
            logger.debug(f"TerminalMappings dataframe loaded with shape: {df_terminals.shape}")
            for index, row in df_terminals.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                condition = row.get("Condition", None)
                logger.debug(f"Processing TerminalMappings row {index}: Raw Value={raw_value}, Standardized Value={standardized_value}, Condition={condition}")
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in TerminalMappings: {row.to_dict()}")
                    continue
                if raw_value not in mappings["terminals"]:
                    mappings["terminals"][raw_value] = []
                mappings["terminals"][raw_value].append({
                    "standardized": standardized_value,
                    "condition": condition if pd.notna(condition) else None
                })
            logger.debug(f"Loaded {len(mappings['terminals'])} terminal mappings from sheet '{terminal_sheet}': {mappings['terminals']}")
        else:
            logger.warning("Terminal mappings sheet not found. Expected 'TerminalMappings', 'Terminals', or 'Terminal Mappings'.")

        logger.debug(f"Total mappings loaded: Suppliers={len(mappings['suppliers'])}, Domains={len(mappings['domain_to_supplier'])}, Position Holders={len(mappings['position_holders'])}, Products={len(mappings['products'])}, Terminals={len(mappings['terminals'])}")
        logger.debug("Exiting load_mappings")
        return mappings
    except Exception as e:
        logger.error(f"Failed to load mappings from {file_path}: {e}")
        logger.debug("Exiting load_mappings with empty mappings due to error")
        return {
            "suppliers": {},
            "domain_to_supplier": {},
            "position_holders": {},
            "products": {},
            "terminals": {}
        }


# --- Extract Position Holder from Terminal ---
def extract_position_holder(terminal: str) -> str:
    logger.debug(f"Entering extract_position_holder with terminal: {terminal}")
    mappings = load_mappings("mappings.xlsx")
    position_holders = mappings["position_holders"]
    terminal_lower = terminal.lower()
    logger.debug(f"Terminal (lowercase): {terminal_lower}, position_holders mappings: {position_holders}")
    for raw_value, standardized_value in position_holders.items():
        logger.debug(f"Checking if raw_value '{raw_value}' (lowercase: '{raw_value.lower()}') is in terminal '{terminal_lower}'")
        if raw_value.lower() in terminal_lower:
            logger.debug(f"Found match: raw_value '{raw_value}' maps to standardized_value '{standardized_value}'")
            logger.debug(f"Exiting extract_position_holder with result: {standardized_value}")
            return standardized_value
    logger.debug("No position holder found, returning empty string")
    logger.debug("Exiting extract_position_holder")
    return ""

# --- Apply Mappings to Rows ---
def apply_mappings(row: Dict, mappings: Dict[str, Dict], is_opis: bool, email_from: str) -> Dict:
    logger.debug(f"Entering apply_mappings with row: {row}, is_opis: {is_opis}, email_from: {email_from}")
    supplier = row.get("Supplier", "")
    logger.debug(f"Initial Supplier: {supplier}")

    # For OPIS emails, supplier should remain blank unless explicitly set in the parsed data
    if not is_opis and supplier in mappings["suppliers"]:
        logger.debug(f"Mapping supplier '{supplier}' to standardized value")
        row["Supplier"] = mappings["suppliers"][supplier]
        logger.debug(f"Translated Supplier: {supplier} -> {row['Supplier']}")

    # Supply: Extract position holder from terminal using the SupplyMappings tab
    terminal = row.get("Terminal", "")
    logger.debug(f"Extracting position holder for terminal: {terminal}")
    supply = extract_position_holder(terminal)
    row["Supply"] = supply
    logger.debug(f"Set Supply to: {supply}")

    # Product mappings
    product = row.get("Product Name", "")
    logger.debug(f"Processing Product Name: {product}")
    product_key = product.replace("Gross ", "") if product.startswith("Gross ") else product
    logger.debug(f"Product key after removing 'Gross ': {product_key}")
    if product_key in mappings["products"]:
        logger.debug(f"Mapping product '{product_key}' to standardized value")
        row["Product Name"] = mappings["products"][product_key]
        logger.debug(f"Translated Product: {product} -> {row['Product Name']}")
    else:
        product_key = product_key.replace("Wholesale ", "")
        logger.debug(f"Product key after removing 'Wholesale ': {product_key}")
        if product_key in mappings["products"]:
            logger.debug(f"Mapping product '{product_key}' to standardized value")
            row["Product Name"] = mappings["products"][product_key]
            logger.debug(f"Translated Product: {product} -> {row['Product Name']}")

    # Terminal mappings
    supplier = row.get("Supplier", "")
    logger.debug(f"Processing Terminal: {terminal}, Supplier: {supplier}")
    if terminal in mappings["terminals"]:
        logger.debug(f"Found terminal '{terminal}' in mappings, applying terminal mappings")
        for mapping in mappings["terminals"][terminal]:
            condition = mapping["condition"]
            logger.debug(f"Checking terminal mapping: standardized={mapping['standardized']}, condition={condition}")
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
    else:
        logger.debug(f"Terminal '{terminal}' not found in mappings, keeping original value")

    logger.debug(f"Final row after mappings: {row}")
    logger.debug("Exiting apply_mappings")
    return row

# --- Utilities ---
def load_env() -> Dict[str, str]:
    logger.debug("Entering load_env")
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
            logger.error(f"Missing environment variable: {key}")
            raise ValueError(f"Missing environment variable: {key}")
    logger.debug(f"Loaded environment variables: {env_vars}")
    logger.debug("Exiting load_env")
    return env_vars

# --- Email Handling ---
def choose_best_content_from_email(msg) -> str:
    logger.debug("Entering choose_best_content_from_email")
    for part in msg.walk():
        filename = part.get_filename()
        if filename and filename.endswith(".txt"):
            try:
                content = part.get_payload(decode=True).decode(errors="ignore")
                if content.strip():
                    logger.info(f"Using .txt attachment: {filename}")
                    logger.debug(f"Content extracted from attachment: {content[:500]}...")
                    logger.debug("Exiting choose_best_content_from_email with .txt content")
                    return content
            except Exception as e:
                logger.error(f"Failed to decode attachment {filename}: {e}")
    body = msg.get_body(preferencelist=("plain"))
    if body:
        content = body.get_content().strip()
        logger.debug(f"Content extracted from email body: {content[:500]}...")
        logger.debug("Exiting choose_best_content_from_email with body content")
        return content
    logger.debug("No suitable content found, returning empty string")
    logger.debug("Exiting choose_best_content_from_email")
    return ""

def clean_email_content(content: str) -> str:
    logger.debug(f"Entering clean_email_content with content length: {len(content)}")
    try:
        content = content.replace("=\n", "").replace("=20", " ")
        logger.debug("Removed email line breaks and =20 characters")
        content = re.sub(r"-{40,}", "", content)
        logger.debug("Removed long dashes")
        content = re.sub(r"\n{3,}", "\n\n", content)
        logger.debug("Reduced multiple newlines to double newlines")
        cleaned = "\n".join(line.strip() for line in content.splitlines())
        logger.debug(f"Cleaned content length: {len(cleaned)}")
        logger.debug(f"Cleaned content (first 500 chars): {cleaned[:500]}...")
        logger.debug("Exiting clean_email_content")
        return cleaned
    except Exception as e:
        logger.error(f"Failed to clean content: {e}")
        logger.debug("Exiting clean_email_content with original content due to error")
        return content

# --- IMAP Functions ---
def fetch_emails(env: Dict[str, str], process_id: str) -> List[Dict[str, str]]:
    logger.debug(f"Entering fetch_emails for process {process_id}")
    emails = []
    try:
        logger.debug(f"Connecting to IMAP server: {env['IMAP_SERVER']}")
        imap_server = imaplib.IMAP4_SSL(env["IMAP_SERVER"])
        logger.debug("Logging into IMAP server")
        imap_server.login(env["IMAP_USERNAME"], env["IMAP_PASSWORD"])
        logger.debug("Selecting INBOX")
        imap_server.select("INBOX")

        since_date = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
        logger.debug(f"Searching for unseen emails since {since_date}")
        _, msg_nums = imap_server.search(None, f'(SINCE "{since_date}") UNSEEN')
        msg_nums_list = msg_nums[0].split()
        logger.info(f"Found {len(msg_nums_list)} unseen emails since {since_date}")

        for num in msg_nums_list:
            logger.debug(f"Fetching email UID {num.decode()}")
            _, data = imap_server.fetch(num, "(RFC822)")
            msg = BytesParser(policy=policy.default).parsebytes(data[0][1])
            content = choose_best_content_from_email(msg)
            subject = msg.get("Subject", "").strip()
            from_addr = msg.get("From", "").strip()
            logger.debug(
                f"Email UID {num.decode()}: Subject={subject}, From={from_addr}, Content length={len(content)}")
            if content:
                emails.append({
                    "uid": num.decode(),
                    "content": content,
                    "subject": subject,
                    "from_addr": from_addr,
                })
                logger.debug(f"Added email UID {num.decode()} to list")

        logger.debug("Logging out of IMAP server")
        imap_server.logout()

    except Exception as e:
        logger.error(f"Failed to fetch emails: {e}")

    logger.info(f"Fetched {len(emails)} emails for process {process_id}")
    logger.debug("Exiting fetch_emails")
    return emails


def mark_email_as_processed(uid: str, env: Dict[str, str]) -> None:
    logger.debug(f"Entering mark_email_as_processed for UID {uid}")
    try:
        logger.debug(f"Connecting to IMAP server: {env['IMAP_SERVER']}")
        imap_server = imaplib.IMAP4_SSL(env["IMAP_SERVER"])
        logger.debug("Logging into IMAP server")
        imap_server.login(env["IMAP_USERNAME"], env["IMAP_PASSWORD"])
        logger.debug("Selecting INBOX")
        imap_server.select("INBOX")
        logger.debug(f"Marking UID {uid} as processed")
        imap_server.store(uid, '+X-GM-LABELS', 'BDE_Processed')
        logger.debug("Logging out of IMAP server")
        imap_server.logout()
        logger.info(f"Marked email {uid} as processed")
    except Exception as e:
        logger.error(f"Failed to mark processed: {e}")
    logger.debug("Exiting mark_email_as_processed")

# --- Grok API Functions ---
def load_prompt(filename: str) -> str:
    logger.debug(f"Entering load_prompt with filename: {filename}")
    prompt_path = PROMPT_DIR / filename
    logger.debug(f"Loading prompt from: {prompt_path}")
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt = f.read()
    logger.debug(f"Loaded prompt (first 500 chars): {prompt[:500]}...")
    logger.debug("Exiting load_prompt")
    return prompt

async def call_grok_api(prompt: str, content: str, env: Dict[str, str], session: aiohttp.ClientSession, process_id: str) -> Optional[str]:
    logger.info(f"Entering call_grok_api for process {process_id}")
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
        logger.info(f"Preparing to make Grok API call for process {process_id}")
        async def make_request():
            logger.info(f"Sending POST request to {api_url}")
            async with session.post(api_url, headers=headers, json=payload, timeout=30) as response:
                logger.info(f"Response status: {response.status}")
                response.raise_for_status()
                data = await response.json()
                logger.info(f"Grok API response received for process {process_id}")
                return data.get("choices", [{}])[0].get("message", {}).get("content", "[]")

        # Create a task for the API request
        request_task = asyncio.create_task(make_request())
        timeout = 35  # Total timeout in seconds
        start_time = datetime.now()
        logger.info(f"API request start time: {start_time}")

        # Wait for the task with a timeout
        try:
            result = await asyncio.wait_for(request_task, timeout=timeout)
            end_time = datetime.now()
            logger.info(f"API request end time: {end_time}, duration: {(end_time - start_time).total_seconds()} seconds")
            logger.info(f"API call completed successfully, result: {result}")
            return result
        except asyncio.TimeoutError:
            logger.error(f"Grok API call timed out at coroutine level after {timeout} seconds for process {process_id}")
            request_task.cancel()
            try:
                await request_task
            except asyncio.CancelledError:
                logger.info("API request task was successfully cancelled")
            return None

    except aiohttp.ClientTimeout:
        logger.error(f"Grok API call timed out at HTTP level after 30 seconds for process {process_id}")
        return None
    except Exception as e:
        logger.error(f"Grok API call failed for process {process_id}: {e}")
        return None

# --- Processing Functions ---
async def process_email_with_delay(email: Dict[str, str], env: Dict[str, str], process_id: str, session: aiohttp.ClientSession) -> Tuple[List[Dict], List[Dict], Optional[Dict]]:
    logger.debug(f"Entering process_email_with_delay for UID {email.get('uid', '?')} in process {process_id}")
    valid_rows, skipped_rows, failed_email = [], [], None
    try:
        logger.debug("Cleaning email content")
        content = clean_email_content(email.get("content", ""))
        if not content:
            logger.error("Empty email content after cleaning")
            raise ValueError("Empty email content")

        logger.debug(f"Cleaned email content for UID {email.get('uid', '?')}: {content[:500]}...")

        logger.debug("Loading mappings")
        mappings = load_mappings("mappings.xlsx")
        logger.debug(f"Mappings loaded: {mappings.keys()}")

        content_lower = content.lower()
        subject_lower = email.get("subject", "").lower()
        logger.debug(f"Content (lowercase, first 500 chars): {content_lower[:500]}...")
        logger.debug(f"Subject (lowercase): {subject_lower}")
        is_opis = (("opis" in content_lower and ("rack" in content_lower or "wholesale" in content_lower)) or \
                   ("opis" in subject_lower and ("rack" in subject_lower or "wholesale" in subject_lower))) and \
                   not ("From:" in content and "wallis" in content_lower)
        logger.debug(f"Email UID {email.get('uid', '?')} classified as {'OPIS' if is_opis else 'Supplier'}")
        logger.debug(f"Email subject: {email.get('subject', '')}")
        logger.debug(f"Email content (first 500 chars): {content[:500]}")
        prompt_file = "opis_chat_prompt.txt" if is_opis else "supplier_chat_prompt.txt"
        prompt_path = PROMPT_DIR / prompt_file
        logger.debug(f"Attempting to load prompt file from: {prompt_path}")
        prompt_chat = load_prompt(prompt_file)
        logger.debug("Prompt loaded successfully")

        # Extract supplier by looking for known domains (only from SupplierMappings tab)
        email_from = email.get("from_addr", "")
        supplier = None
        domain_to_supplier = mappings["domain_to_supplier"]
        logger.info(f"Starting supplier extraction for UID {email.get('uid', '?')}, from_addr: {email_from!r}")

        if email_from:
            # Handle various email formats: "email@domain.com", "Name <email@domain.com>", or malformed
            email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', email_from)
            if email_match:
                email_addr = email_match.group(0).lower()
                logger.info(f"Extracted email address: {email_addr}")
                try:
                    domain = email_addr.split('@')[-1]
                    logger.info(f"Extracted domain: {domain}")
                    logger.info(f"Available domains in domain_to_supplier: {list(domain_to_supplier.keys())}")
                    if domain in domain_to_supplier:
                        mapped_supplier = domain_to_supplier[domain]
                        logger.info(f"Found domain '{domain}' in domain_to_supplier, mapped to: {mapped_supplier}")
                        if domain != "opisnet.com":
                            supplier = mapped_supplier
                            logger.info(f"Supplier identified from from_addr for UID {email.get('uid', '?')}: {email_addr}, Supplier: {supplier}")
                        else:
                            logger.info(f"Domain {domain} identified as OPIS, supplier will be blank")
                    else:
                        logger.info(f"No supplier domain matched in from_addr for UID {email.get('uid', '?')}: {domain}, will check forwarded From: line")
                except Exception as e:
                    logger.warning(f"Failed to extract domain from from_addr for UID {email.get('uid', '?')}: {email_addr}, error: {e}")
            else:
                logger.warning(f"No valid email address found in from_addr for UID {email.get('uid', '?')}: {email_from!r}")

        if supplier is None and "From:" in content:
            logger.info("Checking for forwarded From: line in content")
            forwarded_from_matches = re.finditer(r"From:\s*(.*?)\s*(?:<([^>]+)>|$)", content, re.IGNORECASE)
            for match in forwarded_from_matches:
                name, email_addr = match.groups()
                if email_addr:
                    email_addr = email_addr.lower()
                    logger.info(f"Found forwarded From: line with email: {email_addr}")
                    try:
                        domain = email_addr.split('@')[-1]
                        logger.info(f"Extracted domain from forwarded From: {domain}")
                        if domain in domain_to_supplier:
                            mapped_supplier = domain_to_supplier[domain]
                            logger.info(f"Found domain '{domain}' in domain_to_supplier, mapped to: {mapped_supplier}")
                            if domain != "opisnet.com":
                                supplier = mapped_supplier
                                logger.info(f"Supplier identified from forwarded From: line for UID {email.get('uid', '?')}: {email_addr}, Supplier: {supplier}")
                                break
                            else:
                                logger.info(f"Forwarded From: domain {domain} is OPIS, skipping as supplier")
                        else:
                            logger.info(f"No supplier domain matched in forwarded From: line for UID {email.get('uid', '?')}: {domain}, continuing search")
                    except Exception as e:
                        logger.warning(f"Failed to extract domain from forwarded From: line for UID {email.get('uid', '?')}: {email_addr}, error: {e}")
                else:
                    logger.info(f"No email address in forwarded From: line for UID {email.get('uid', '?')}, name: {name}")

        if supplier is None:
            supplier = "Unknown Supplier"
            logger.warning(f"Could not identify supplier for UID {email.get('uid', '?')}, using default: {supplier}")

        logger.debug(f"Supplier extraction complete, supplier: {supplier}")

        # Log before calling Grok API to confirm we reach this point
        logger.info(f"Calling Grok API for UID {email.get('uid', '?')} in process {process_id}")
        parsed = await call_grok_api(prompt_chat, content, env, session, process_id)
        logger.info(f"Grok API call returned for UID {email.get('uid', '?')}")
        if parsed is None:
            logger.error("Grok API returned None")
            raise ValueError("Grok API returned None")
        logger.debug(f"Grok raw response for UID {email.get('uid', '?')}: {parsed}")

        if parsed.startswith("```json"):
            logger.debug("Extracting JSON from markdown code block")
            match = re.search(r"```json\s*(.*?)\s*```", parsed, re.DOTALL)
            if match:
                parsed = match.group(1).strip()
                logger.debug(f"Extracted JSON: {parsed}")
            else:
                logger.error("Failed to extract JSON from Grok response")
                raise ValueError("Failed to extract JSON from Grok response")

        logger.debug("Parsing JSON response")
        try:
            rows = json.loads(parsed)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Grok response as JSON for UID {email.get('uid', '?')}: {parsed}")
            raise ValueError(f"Invalid JSON in Grok response: {str(e)}")

        logger.debug(f"Parsed {len(rows)} rows from email UID {email.get('uid', '?')}: {rows}")
        for row in rows:
            logger.debug(f"Processing row: {row}")
            if not isinstance(row, dict):
                logger.debug(f"Skipping invalid row (not a dict): {row}")
                continue
            if not row.get("Product Name") or not row.get("Terminal") or not isinstance(row.get("Price"), (int, float)):
                logger.debug(f"Skipping row due to missing or invalid required fields: {row}")
                continue
            price = row.get("Price", 0)
            logger.debug(f"Checking price: {price}")
            if price > 5:
                price = price / 100
                row["Price"] = price
                logger.debug(f"Converted price from cents to dollars: {row}")
            if price > 5:
                logger.debug(f"Skipping row due to price > 5: {row}")
                continue
            if not is_opis:
                logger.debug(f"Setting Supplier to {supplier} for non-OPIS email")
                row["Supplier"] = supplier
            else:
                logger.debug("OPIS email, leaving Supplier blank")
            logger.debug(f"Applying mappings to row: {row}")
            row = apply_mappings(row, mappings, is_opis, email_from=supplier)
            logger.debug(f"Row after mappings: {row}")
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
            logger.debug(f"Added row to valid_rows: {valid_rows[-1]}")

        if valid_rows:
            logger.debug(f"Marking email UID {email.get('uid', '?')} as processed")
            mark_email_as_processed(email.get("uid", ""), env)
            logger.info(f"Successfully parsed {len(valid_rows)} valid rows from email UID {email.get('uid', '?')}")
            # Save process status after successfully processing an email
            process_statuses = load_process_status(process_id) or {}
            process_statuses["row_count"] = (process_statuses.get("row_count", 0) + len(valid_rows))
            logger.debug(f"Updating process status with row_count: {process_statuses['row_count']}")
            save_process_status(process_id, process_statuses)

    except Exception as ex:
        failed_email = {"email_id": email.get("uid", "?"), "subject": email.get("subject", ""), "from_addr": email.get("from_addr", ""), "error": str(ex)}
        logger.error(f"Failed to process email UID {email.get('uid', '?')}: {str(ex)}")
        # Save process status on failure
        process_statuses = load_process_status(process_id) or {}
        process_statuses["error"] = str(ex)
        logger.debug("Saving process status on failure")
        save_process_status(process_id, process_statuses)

    logger.debug(f"Exiting process_email_with_delay with valid_rows: {len(valid_rows)}, skipped_rows: {len(skipped_rows)}, failed_email: {failed_email}")
    return valid_rows, skipped_rows, failed_email

async def process_all_emails(process_id: str, process_statuses: Dict[str, dict]) -> None:
    logger.info(f"Parser.py version: 2025-04-29 with domain_to_supplier fix, API timeout, increased delay, debug logging, and local logging fix")
    file_handler = setup_file_logging(process_id)
    try:
        logger.debug("Loading environment variables")
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
        logger.debug("Saving initial process status")
        save_process_status(process_id, initial_status)
        logger.info(f"Started process {process_id}, saved initial status")

        logger.debug("Fetching emails")
        emails = fetch_emails(env, process_id)
        process_statuses[process_id]["email_count"] = len(emails)
        logger.debug(f"Saving process status with email_count: {len(emails)}")
        save_process_status(process_id, process_statuses[process_id])
        logger.info(f"Fetched {len(emails)} emails for process {process_id}")

        if not emails:
            process_statuses[process_id]["status"] = "done"
            logger.debug("No emails to process, saving done status")
            save_process_status(process_id, process_statuses[process_id])
            logger.info(f"No emails to process for {process_id}")
            return

        output_file = f"parsed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        total_rows = 0
        failed_emails = []

        logger.debug("Creating aiohttp ClientSession")
        async with aiohttp.ClientSession() as session:
            for idx, email in enumerate(emails):
                process_statuses[process_id]["current_email"] = idx + 1
                logger.debug(f"Saving process status with current_email: {idx + 1}")
                save_process_status(process_id, process_statuses[process_id])
                logger.info(f"Processing email {idx + 1}/{len(emails)} for process {process_id}")
                logger.debug(f"Email details: {email}")
                valid_rows, skipped_rows, failed_email = await process_email_with_delay(email, env, process_id, session)

                if valid_rows:
                    logger.debug(f"Saving {len(valid_rows)} valid rows to CSV")
                    save_to_csv(valid_rows, output_file, process_id)
                    total_rows += len(valid_rows)
                    process_statuses[process_id]["row_count"] = total_rows
                    logger.debug(f"Saving process status with row_count: {total_rows}")
                    save_process_status(process_id, process_statuses[process_id])

                if failed_email:
                    failed_email["content"] = email.get("content", "")
                    failed_emails.append(failed_email)
                    logger.debug(f"Added failed email: {failed_email}")

                logger.debug("Sleeping for 5 seconds to avoid API rate limits")
                await asyncio.sleep(5)

        if failed_emails:
            logger.debug(f"Saving {len(failed_emails)} failed emails to CSV")
            save_failed_emails_to_csv(failed_emails, output_file, process_id)

        process_statuses[process_id]["status"] = "done"
        process_statuses[process_id]["output_file"] = output_file
        logger.debug("Saving final process status as done")
        save_process_status(process_id, process_statuses[process_id])
        logger.info(f"Completed process {process_id} with {total_rows} rows")

        logger.debug("Sleeping for 300 seconds before deleting process status")
        await asyncio.sleep(300)
        logger.debug("Deleting process status")
        delete_process_status(process_id)
    except Exception as e:
        logger.error(f"Error in process_all_emails for process {process_id}: {str(e)}")
        process_statuses[process_id]["status"] = "error"
        process_statuses[process_id]["error"] = str(e)
        logger.debug("Saving process status on error")
        save_process_status(process_id, process_statuses[process_id])
    finally:
        logger.debug("Removing file logging handler")
        remove_file_logging(file_handler)

# --- CSV Saving Functions ---
def save_to_csv(data: List[Dict], output_filename: str, process_id: str) -> None:
    logger.debug(f"Entering save_to_csv with {len(data)} rows, output_filename: {output_filename}")
    try:
        output_path = OUTPUT_DIR / output_filename
        fieldnames = ["Supplier", "Supply", "Product Name", "Terminal", "Price", "Volume Type", "Effective Date", "Effective Time"]
        mode = "a" if output_path.exists() else "w"
        logger.debug(f"Opening file {output_path} in mode {mode}")
        with open(output_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if mode == "w":
                logger.debug("Writing CSV header")
                writer.writeheader()
            for row in data:
                logger.debug(f"Writing row: {row}")
                writer.writerow(row)
        logger.debug("Exiting save_to_csv")
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")
        logger.debug("Exiting save_to_csv due to error")

def save_failed_emails_to_csv(failed_emails: List[Dict], output_filename: str, process_id: str) -> None:
    logger.debug(f"Entering save_failed_emails_to_csv with {len(failed_emails)} failed emails")
    try:
        if failed_emails:
            failed_filename = f"failed_{output_filename}"
            failed_path = OUTPUT_DIR / failed_filename
            logger.debug(f"Saving failed emails to {failed_path}")
            df_failed = pd.DataFrame(failed_emails)
            df_failed.to_csv(failed_path, index=False)
            logger.debug("Failed emails saved successfully")
        else:
            logger.debug("No failed emails to save")
        logger.debug("Exiting save_failed_emails_to_csv")
    except Exception as e:
        logger.error(f"Failed to save failed emails: {e}")
        logger.debug("Exiting save_failed_emails_to_csv due to error")