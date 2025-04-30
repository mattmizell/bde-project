import os
import re
import imaplib
import aiohttp
import asyncio
import pandas as pd
import json
import csv
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from email import policy
from email.parser import BytesParser
from rapidfuzz import fuzz
import logging


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
        mappings = {
            "suppliers": {},
            "domain_to_supplier": {},
            "position_holders": {},
            "products": {},
            "terminals": {},
            "supply_lookup": {}
        }
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
                domain = str(row.get("Domain", "")).strip()
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in SupplierMappings: {row.to_dict()}")
                    continue
                mappings["suppliers"][raw_value] = standardized_value
                if domain:
                    mappings["domain_to_supplier"][domain.lower()] = standardized_value
            logger.debug(f"Loaded {len(mappings['suppliers'])} suppliers, {len(mappings['domain_to_supplier'])} domain mappings")
        else:
            logger.warning("Supplier mappings sheet not found.")

        # Load SupplyMappings
        supply_sheet = None
        for sheet_name in ["SupplyMappings", "Supply", "Supply Mappings"]:
            if sheet_name in xl.sheet_names:
                supply_sheet = sheet_name
                break
        if supply_sheet:
            logger.debug(f"Loading SupplyMappings from sheet: {supply_sheet}")
            df_supply = xl.parse(supply_sheet)
            for index, row in df_supply.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in SupplyMappings: {row.to_dict()}")
                    continue
                mappings["position_holders"][raw_value] = standardized_value
            logger.debug(f"Loaded {len(mappings['position_holders'])} position holder mappings")
        else:
            logger.warning("Supply mappings sheet not found.")

        # Load ProductMappings
        product_sheet = None
        for sheet_name in ["ProductMappings", "Products", "Product Mappings"]:
            if sheet_name in xl.sheet_names:
                product_sheet = sheet_name
                break
        if product_sheet:
            logger.debug(f"Loading ProductMappings from sheet: {product_sheet}")
            df_products = xl.parse(product_sheet)
            for index, row in df_products.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in ProductMappings: {row.to_dict()}")
                    continue
                mappings["products"][raw_value] = standardized_value
            logger.debug(f"Loaded {len(mappings['products'])} product mappings")
        else:
            logger.warning("Product mappings sheet not found.")

        # Load TerminalMappings
        terminal_sheet = None
        for sheet_name in ["TerminalMappings", "Terminals", "Terminal Mappings"]:
            if sheet_name in xl.sheet_names:
                terminal_sheet = sheet_name
                break
        if terminal_sheet:
            logger.debug(f"Loading TerminalMappings from sheet: {terminal_sheet}")
            df_terminals = xl.parse(terminal_sheet)
            for index, row in df_terminals.iterrows():
                raw_value = str(row["Raw Value"]).strip()
                standardized_value = str(row["Standardized Value"]).strip()
                condition = row.get("Condition", None)
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in TerminalMappings: {row.to_dict()}")
                    continue
                if raw_value not in mappings["terminals"]:
                    mappings["terminals"][raw_value] = []
                mappings["terminals"][raw_value].append({
                    "standardized": standardized_value,
                    "condition": condition if pd.notna(condition) else None
                })
            logger.debug(f"Loaded {len(mappings['terminals'])} terminal mappings")
        else:
            logger.warning("Terminal mappings sheet not found.")

        # ✅ Load SupplyLookupMappings
        supply_lookup_sheet = None
        for sheet_name in ["SupplyLookupMappings", "Supply Prefixes", "Supply Lookup"]:
            if sheet_name in xl.sheet_names:
                supply_lookup_sheet = sheet_name
                break
        if supply_lookup_sheet:
            logger.debug(f"Loading SupplyLookupMappings from sheet: {supply_lookup_sheet}")
            df_lookup = xl.parse(supply_lookup_sheet)
            for index, row in df_lookup.iterrows():
                raw_prefix = row.get("Prefix")
                raw_supply = row.get("Supply")

                if pd.notna(raw_prefix) and pd.notna(raw_supply):
                    prefix = str(raw_prefix).strip()
                    supply = str(raw_supply).strip()
                    if prefix and supply:
                        mappings["supply_lookup"][prefix] = supply
                    else:
                        logger.warning(f"⚠️ Empty after strip in SupplyLookupMappings: {row.to_dict()}")
                else:
                    logger.warning(f"⚠️ Skipping row with NaN in SupplyLookupMappings: {row.to_dict()}")
            logger.info(f"Loaded {len(mappings['supply_lookup'])} supply prefix mappings")
        else:
            logger.warning("SupplyLookupMappings sheet not found.")

        logger.debug("Exiting load_mappings")
        return mappings

    except Exception as e:
        logger.error(f"Failed to load mappings from {file_path}: {e}")
        return {
            "suppliers": {},
            "domain_to_supplier": {},
            "position_holders": {},
            "products": {},
            "terminals": {},
            "supply_lookup": {}
        }



# --- Resolve Supply (NEW FUNCTION) ---
def resolve_supply(terminal: str, supply_mappings: dict, fuzzy_threshold: int = 80) -> str:
    """
    Attempts to resolve a standardized supply name from a given terminal string.
    - First uses deterministic prefix matching from supply_mappings.
    - Then uses fuzzy string matching (via rapidfuzz) if no prefix matches.
    Returns "Unknown Supply" if no reliable match is found.
    """
    logger.debug(f"🔍 Resolving supply for terminal: '{terminal}'")

    if not terminal or not isinstance(terminal, str):
        logger.warning("⚠️ Terminal is missing or invalid; returning Unknown Supply")
        return "Unknown Supply"

    terminal = terminal.strip()

    # --- Deterministic Prefix Match ---
    for prefix, supply in supply_mappings.items():
        if terminal.startswith(prefix):
            logger.info(f"✅ Prefix match: terminal '{terminal}' starts with '{prefix}' → '{supply}'")
            return supply

    # --- Fuzzy Matching Fallback ---
    best_score = 0
    best_match = None
    for prefix, supply in supply_mappings.items():
        score = fuzz.partial_ratio(prefix.lower(), terminal.lower())
        logger.debug(f"🤖 Fuzzy comparing '{prefix}' with '{terminal}' → score: {score}")
        if score > best_score:
            best_score = score
            best_match = supply

    if best_score >= fuzzy_threshold:
        logger.info(f"🧠 Fuzzy match accepted: '{terminal}' → '{best_match}' (score: {best_score})")
        return best_match
    else:
        logger.warning(f"❌ No supply match found for terminal: '{terminal}', returning 'Unknown Supply'")
        return "Unknown Supply"



# --- Apply Mappings to Rows ---
def apply_mappings(row: Dict, mappings: Dict[str, Dict], is_opis: bool, email_from: str) -> Dict:
    logger.debug(f"Entering apply_mappings with row: {row}, is_opis: {is_opis}, email_from: {email_from}")

    # --- Supplier ---
    supplier = row.get("Supplier", "")
    if not is_opis and supplier in mappings["suppliers"]:
        row["Supplier"] = mappings["suppliers"][supplier]
        logger.debug(f"Mapped Supplier: {supplier} → {row['Supplier']}")

    # --- Supply ---
    terminal = row.get("Terminal", "")
    supply = row.get("Supply", "").strip()

    if not supply or supply.lower() == "unknown supply":
        # First try fuzzy/position_holder match
        supply = resolve_supply(terminal, mappings.get("position_holders", {}))
        logger.debug(f"resolve_supply fallback returned: {supply}")

        # Then token-based prefix fallback (e.g., FH-MG-KANSAS CITY → FH)
        if not supply or supply.lower() == "unknown supply":
            prefix_token = terminal.split("-")[0].strip().upper()
            supply_from_lookup = mappings.get("supply_lookup", {}).get(prefix_token)
            if supply_from_lookup:
                supply = supply_from_lookup
                logger.info(f"✅ Overriding missing/unknown supply using prefix '{prefix_token}': {supply_from_lookup}")
            else:
                logger.warning(f"❌ No supply match found for terminal prefix: '{prefix_token}', keeping supply")

    row["Supply"] = supply

    # --- Product ---
    product = row.get("Product Name", "")
    product_key = product.replace("Gross ", "") if product.startswith("Gross ") else product
    if product_key in mappings["products"]:
        row["Product Name"] = mappings["products"][product_key]
    else:
        product_key = product_key.replace("Wholesale ", "")
        if product_key in mappings["products"]:
            row["Product Name"] = mappings["products"][product_key]

    # --- Terminal ---
    terminal_map = mappings.get("terminals", {}).get(terminal, [])
    supplier = row.get("Supplier", "")
    for mapping in terminal_map:
        condition = mapping.get("condition")
        if condition is None:
            row["Terminal"] = mapping["standardized"]
            break
        elif condition == 'Supplier in ["Phillips 66", "Cenex"]' and supplier in ["Phillips 66", "Cenex"]:
            row["Terminal"] = mapping["standardized"]
            break
        elif condition == 'Supplier not in ["Phillips 66", "Cenex"]' and supplier not in ["Phillips 66", "Cenex"]:
            row["Terminal"] = mapping["standardized"]
            break

    logger.debug(f"Final row after mappings: {row}")
    logger.debug("Exiting apply_mappings")
    return row



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

def split_content_into_chunks(content: str, max_length: int = 6000) -> List[str]:
    """
    Split large text content into chunks not exceeding max_length.
    Prefers to split at line breaks to preserve structure.
    """
    lines = content.splitlines(keepends=True)
    chunks = []
    current_chunk = ""

    for line in lines:
        if len(current_chunk) + len(line) <= max_length:
            current_chunk += line
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = line

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks

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
    logger.info(f"Loading prompt from file: {filename}")
    prompt_path = PROMPT_DIR / filename
    logger.info(f"Resolved prompt path: {prompt_path}")
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt = f.read()
    logger.info(f"Loaded prompt (first 200 chars): {prompt[:200].replace(chr(10), ' ')}...")
    return prompt


async def call_grok_api(prompt: str, content: str, env: Dict[str, str], session: aiohttp.ClientSession, process_id: str) -> Optional[str]:
    logger.info(f"Entering call_grok_api for process {process_id}")

    try:
        api_url = "https://api.x.ai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {env['XAI_API_KEY']}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": env.get("MODEL", "grok-3-latest"),
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": content}
            ]
        }

        # ✅ Logging details before the call
        logger.info(f"Prompt loaded (first 200 chars): {prompt[:200].replace(chr(10), ' ')}")
        logger.info(f"Content to parse (first 200 chars): {content[:200].replace(chr(10), ' ')}")
        logger.debug(f"Full prompt:\n{prompt}")
        logger.debug(f"Full content:\n{content}")
        logger.debug(f"API payload:\n{json.dumps(payload, indent=2)}")

        async def make_request():
            logger.info(f"Sending POST request to {api_url}")
            async with session.post(api_url, headers=headers, json=payload, timeout=90) as response:
                logger.info(f"Response status: {response.status}")
                response.raise_for_status()
                data = await response.json()
                logger.info(f"Grok API response received for process {process_id}")
                return data.get("choices", [{}])[0].get("message", {}).get("content", "[]")

        timeout = 95  # Increased coroutine timeout
        start_time = datetime.now()
        logger.info(f"API request start time: {start_time}")

        try:
            result = await asyncio.wait_for(make_request(), timeout=timeout)
            end_time = datetime.now()
            logger.info(f"API request end time: {end_time}, duration: {(end_time - start_time).total_seconds()} seconds")
            logger.info(f"API call completed successfully, result: {result[:200]}...")
            return result
        except asyncio.TimeoutError:
            logger.error(f"Grok API call timed out at coroutine level after {timeout} seconds for process {process_id}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while waiting for Grok API: {e}")
            return None

    except aiohttp.ClientTimeout:
        logger.error(f"Grok API call timed out at HTTP level after 90 seconds for process {process_id}")
        return None
    except Exception as e:
        logger.error(f"Grok API call failed for process {process_id}: {e}")
        return None



# --- Processing Functions ---
async def process_email_with_delay(
    email: Dict[str, str],
    env: Dict[str, str],
    process_id: str,
    session: aiohttp.ClientSession
) -> Tuple[List[Dict], List[Dict], Optional[Dict]]:
    logger.debug(f"Entering process_email_with_delay for UID {email.get('uid', '?')} in process {process_id}")

    valid_rows = []
    skipped_rows = []
    failed_email = None

    try:
        content = clean_email_content(email.get("content", ""))
        if not content:
            raise ValueError("Empty email content")

        logger.info(f"Email content length after cleaning: {len(content)} characters")

        # Chunk content if needed
        chunks = split_content_into_chunks(content, max_length=6000)
        logger.info(f"Split content into {len(chunks)} chunks")

        mappings = load_mappings("mappings.xlsx")
        domain_to_supplier = {k.strip().lower(): v.strip() for k, v in mappings.get("domain_to_supplier", {}).items()}

        content_lower = content.lower()
        subject_lower = email.get("subject", "").lower()
        is_opis = ("opis" in content_lower and ("rack" in content_lower or "wholesale" in content_lower)) or \
                  ("opis" in subject_lower and ("rack" in subject_lower or "wholesale" in subject_lower))

        prompt_file = "opis_chat_prompt.txt" if is_opis else "supplier_chat_prompt.txt"
        prompt_chat = load_prompt(prompt_file)

        email_from = email.get("from_addr", "")
        supplier = None

        if email_from:
            email_match = re.search(r"[\w\.-]+@[\w\.-]+", email_from)
            if email_match:
                domain = email_match.group(0).split("@")[-1].strip().lower()
                logger.info(f"Parsed domain from from_addr: {domain}")
                supplier = domain_to_supplier.get(domain)

        if not supplier and "From:" in content:
            forwarded_matches = re.finditer(r"From:\s*(.*?)\s*(?:<([^>]+)>|$)", content, re.IGNORECASE)
            for match in forwarded_matches:
                _, email_addr = match.groups()
                if email_addr:
                    domain = email_addr.split("@")[-1].strip().lower()
                    logger.info(f"Parsed domain from forwarded From: {domain}")
                    supplier = domain_to_supplier.get(domain)
                    if supplier:
                        break

        if not supplier:
            supplier = "Unknown Supplier"
            logger.warning(f"No supplier identified for UID {email.get('uid', '?')}, defaulting to Unknown Supplier")

        parsed_rows = []

        for idx, chunk in enumerate(chunks):
            logger.info(f"Calling Grok API for chunk {idx+1}/{len(chunks)} for UID {email.get('uid', '?')}")
            parsed = await call_grok_api(prompt_chat, chunk, env, session, process_id)

            if parsed is None:
                logger.warning(f"Grok API returned None for chunk {idx+1} of UID {email.get('uid', '?')}")
                continue

            if parsed.startswith("```json"):
                parsed = re.sub(r"```json|```", "", parsed, flags=re.DOTALL).strip()

            try:
                rows = json.loads(parsed)
                if not isinstance(rows, list):
                    raise ValueError(f"Grok response not a list for chunk {idx+1}")
                parsed_rows.extend(rows)
            except Exception as ex:
                logger.error(f"Failed to parse Grok response for chunk {idx+1}: {ex}")
                continue

        logger.info(f"Total parsed rows across all chunks: {len(parsed_rows)} for UID {email.get('uid', '?')}")

        for row in parsed_rows:
            if not isinstance(row, dict):
                continue
            if not row.get("Product Name") or not row.get("Terminal") or not isinstance(row.get("Price"), (int, float)):
                continue

            price = row.get("Price", 0)
            if price > 5:
                price = price / 100
                row["Price"] = price

            if price > 5:
                continue

            if not is_opis:
                if not row.get("Supplier"):
                    row["Supplier"] = supplier
                if not row.get("Supply"):
                    row["Supply"] = supplier

            row = apply_mappings(row, mappings, is_opis, email_from=supplier)

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
            logger.info(f"Parsed {len(valid_rows)} valid rows from email UID {email.get('uid', '?')}")

    except Exception as ex:
        failed_email = {
            "email_id": email.get("uid", "?"),
            "subject": email.get("subject", ""),
            "from_addr": email.get("from_addr", ""),
            "error": str(ex),
        }
        logger.error(f"Failed to process email UID {email.get('uid', '?')}: {str(ex)}")

    logger.debug(f"Exiting process_email_with_delay with {len(valid_rows)} valid rows")
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