import re
import email
from difflib import get_close_matches

def regex_extract(pattern, text, flags=0):
    match = re.search(pattern, text, flags)
    return match.group(1).strip() if match else None

def fuzzy_match(value, options, cutoff=0.8):
    matches = get_close_matches(value, options, n=1, cutoff=cutoff)
    return matches[0] if matches else None

import os
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

from email import policy
# Trigger render redeploy
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
# Trigger render redeploy

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
                logger.debug(f"Loaded status for process {process_id} from file: {status}")
                return status
        else:
            # Fallback to in-memory dict
            fallback_status = process_status.get(process_id)
            if fallback_status:
                logger.warning(f"Status file not found for {process_id}, falling back to in-memory status")
                return fallback_status
            else:
                logger.warning(f"No file or in-memory status found for {process_id}")
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
                domain = str(row.get("Domain", "")).strip().lower()
                if pd.isna(raw_value) or pd.isna(standardized_value):
                    logger.warning(f"Skipping invalid row in SupplierMappings: {row.to_dict()}")
                    continue
                mappings["suppliers"][raw_value] = standardized_value
                if domain:
                    mappings["domain_to_supplier"][domain] = standardized_value
                    logger.debug(f"📫 Added domain mapping: {domain} → {standardized_value}")
            logger.info(f"✅ Loaded {len(mappings['suppliers'])} supplier names")
            logger.info(f"✅ Loaded {len(mappings['domain_to_supplier'])} domain-to-supplier mappings")
        else:
            logger.warning("❌ Supplier mappings sheet not found.")

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
            logger.info(f"✅ Loaded {len(mappings['position_holders'])} position holder mappings")
        else:
            logger.warning("❌ Supply mappings sheet not found.")

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
            logger.info(f"✅ Loaded {len(mappings['products'])} product mappings")
        else:
            logger.warning("❌ Product mappings sheet not found.")

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
            logger.info(f"✅ Loaded {len(mappings['terminals'])} terminal mappings")
        else:
            logger.warning("❌ Terminal mappings sheet not found.")

        # Load SupplyLookupMappings
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
                        logger.debug(f"🔗 Supply prefix mapping: {prefix} → {supply}")
                    else:
                        logger.warning(f"⚠️ Empty after strip in SupplyLookupMappings: {row.to_dict()}")
                else:
                    logger.warning(f"⚠️ Skipping row with NaN in SupplyLookupMappings: {row.to_dict()}")
            logger.info(f"✅ Loaded {len(mappings['supply_lookup'])} supply prefix mappings")
        else:
            logger.warning("❌ SupplyLookupMappings sheet not found.")

        logger.debug("Exiting load_mappings")
        return mappings

    except Exception as e:
        logger.error(f"❌ Failed to load mappings from {file_path}: {e}")
        return {
            "suppliers": {},
            "domain_to_supplier": {},
            "position_holders": {},
            "products": {},
            "terminals": {},
            "supply_lookup": {}
        }



# --- Prompt Helpers ---
def supply_examples_prompt_block(position_holders: Dict[str, str]) -> str:
    """
    Converts SupplyMappings (position_holders) into prompt-friendly examples
    to help Grok infer the correct Supply from raw terminal strings.
    """
    if not position_holders:
        return ""

    examples = []
    for raw_value, standardized_value in position_holders.items():
        examples.append(
            f"Example:\nRaw Terminal: {raw_value}\nResolved Position Holder (Supply): {standardized_value}"
        )

    block = "\n\n### SUPPLY RESOLUTION EXAMPLES\n\n" + "\n\n".join(examples)
    return block

def supply_lookup_prompt_block(supply_lookup: Dict[str, str]) -> str:
    """
    Converts SupplyLookupMappings into a prompt-friendly block to help Grok infer Supply from terminal prefixes.
    Example: FH-MG → Flint Hills
    """
    if not supply_lookup:
        return ""

    examples = [
        f"Prefix: {prefix} → Supply: {supply}"
        for prefix, supply in supply_lookup.items()
    ]

    block = "\n\n### SUPPLY LOOKUP PREFIX EXAMPLES\n\n" + "\n".join(examples)
    return block

def terminal_mapping_prompt_block(terminal_mappings: Dict[str, List[Dict]]) -> str:
    """
    Converts TerminalMappings into a readable format for prompt injection.
    Example: Raw → Standardized (with condition, if any)
    """
    if not terminal_mappings:
        return ""

    lines = []
    for raw, entries in terminal_mappings.items():
        for entry in entries:
            standardized = entry.get("standardized", "")
            condition = entry.get("condition", "")
            if condition:
                lines.append(f"Raw Terminal: {raw} → Standardized: {standardized} (Condition: {condition})")
            else:
                lines.append(f"Raw Terminal: {raw} → Standardized: {standardized}")

    block = "\n\n### TERMINAL MAPPING EXAMPLES\n\n" + "\n".join(lines)
    return block

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

    if terminal:
        terminal = terminal.strip()
    else:
        terminal = ""

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
    supply = str(row.get("Supply", "") or "").strip()

    if not supply or supply.lower() == "unknown supply":
        # First try fuzzy/position_holder match
        supply = resolve_supply(terminal, mappings.get("position_holders", {}))
        logger.debug(f"resolve_supply fallback returned: {supply}")

        # Then token-based prefix fallback (e.g., FH-MG-KANSAS CITY → FH)
        if not supply or supply.lower() == "unknown supply":
            if terminal:
                prefix_token = terminal.split("-")[0].strip().upper()
            else:
                prefix_token = ""
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

# --- Utilities ---
def extract_domain_from_forwarded_headers(content: str, domain_to_supplier: Dict[str, str]) -> Optional[str]:
    """
    Scans the email content for forwarded headers and extracts a simplified domain
    to match against known supplier domains. Supports lines like "From: Name <email@domain.com>".
    """
    logger = logging.getLogger("parser")
    forwarded_lines = re.findall(r"(?i)^From:\s*(.*)", content, re.MULTILINE)

    for line in forwarded_lines:
        match = re.search(r"[\w\.-]+@[\w\.-]+", line)
        if match:
            full_domain = match.group(0).split("@")[-1].strip().lower()
            logger.info(f"Parsed forwarded domain: {full_domain}")

            # Try full domain first
            if full_domain in domain_to_supplier:
                return domain_to_supplier[full_domain]

            # Fallback to simplified base domain (e.g., wallis.com from mail.wallis.com)
            parts = full_domain.split(".")
            if len(parts) >= 2:
                base_domain = ".".join(parts[-2:])  # get last two segments
                logger.info(f"Trying base domain fallback: {base_domain}")
                if base_domain in domain_to_supplier:
                    return domain_to_supplier[base_domain]

    return None

def extract_domains_from_body(content: str, domain_to_supplier: Dict[str, str]) -> Optional[str]:
    """
    Scan all lines in email body to find company domains (exclude known relay domains)
    and attempt to match them to known suppliers using exact or fallback domain logic.
    """
    known_relays = {"outlook.com", "gmail.com", "yahoo.com", "hotmail.com", "icloud.com"}
    seen_domains = set()

    logger.debug("📬 RAW EMAIL BODY START")
    logger.debug(content)
    logger.debug("📬 RAW EMAIL BODY END")

    logger.debug("🔎 Searching for @-based domains in email body")

    for match in re.findall(r'[\w\.-]+@([\w\.-]+\.\w+)', content):
        domain = match.lower().strip()
        logger.debug(f"Found domain candidate: {domain}")

        if domain in known_relays:
            continue

        # Exact match
        if domain in domain_to_supplier:
            logger.debug(f"✅ Exact match for domain: {domain}")
            return domain_to_supplier[domain]

        # Fallback match by base domain
        parts = domain.split('.')
        if len(parts) > 2:
            base_domain = '.'.join(parts[-2:])
            logger.debug(f"Trying fallback base domain: {base_domain}")
            if base_domain in domain_to_supplier:
                logger.debug(f"✅ Fallback match: {base_domain}")
                return domain_to_supplier[base_domain]

        seen_domains.add(domain)

    logger.warning(f"❌ No domain match found in body. Domains seen: {seen_domains}")
    return None


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
                    content = clean_email_content(content)
                    logger.info(f"Using .txt attachment: {filename}")
                    logger.debug(f"Content extracted from attachment: {content[:500]}...")
                    logger.debug("Exiting choose_best_content_from_email with .txt content")
                    return content
            except Exception as e:
                logger.error(f"Failed to decode attachment {filename}: {e}")

    body = msg.get_body(preferencelist=("plain"))
    if body:
        content = body.get_content().strip()
        content = clean_email_content(content)
        logger.debug(f"Content extracted from email body: {content[:500]}...")
        logger.debug("Exiting choose_best_content_from_email with body content")
        return content

    logger.debug("No suitable content found, returning empty string")
    return ""



def clean_email_content(content: str) -> str:
    """
    Cleans email text to improve parsing while preserving important metadata lines.
    - Preserves lines with headers like "From:", "To:", "Subject:" and email addresses.
    - Normalizes tables and spacing for better Grok parsing.
    """
    if not content:
        return ""

    lines = content.splitlines()
    preserved_lines = []
    cleaned_lines = []

    for line in lines:
        stripped = line.strip()

        # Preserve metadata lines or lines with email addresses
        if (
            stripped.lower().startswith(("from:", "to:", "subject:")) or
            re.search(r"[\w\.-]+@[\w\.-]+\.\w+", stripped)
        ):
            preserved_lines.append(stripped)
            continue

        # Normalize common encoding artifacts
        if stripped.endswith("=\n"):
            stripped = stripped.replace("=\n", "")
        if "=20" in stripped:
            stripped = stripped.replace("=20", " ")

        # Replace tabs with spaces and normalize whitespace in potential tables
        if '\t' in stripped or re.search(r'\s{2,}', stripped):
            stripped = re.sub(r'\s{2,}', '\t', stripped.replace('\t', ' '))

        if stripped and not re.match(r"^[-_=]{5,}$", stripped):
            cleaned_lines.append(stripped)

    logger.debug("Preserved metadata lines:\n" + "\n".join(preserved_lines[:10]))
    logger.debug("Cleaned table/body lines:\n" + "\n".join(cleaned_lines[:10]))

    return "\n".join(preserved_lines + cleaned_lines)



import re

def split_content_into_chunks(content: str, max_chunk_size: int = 2000) -> List[str]:
    """
    Smart splitter: tries to split by sections (double newlines or headers), then by size.
    """
    # Step 1: Try to split by logical sections (double newlines or headers)
    # You can refine this with regex for supplier/terminal/product headers
    sections = re.split(r"\n\s*\n", content)
    chunks = []
    current_chunk = ""

    for section in sections:
        section = section.strip()
        if not section:
            continue

        # If adding this section would exceed max size, flush current chunk
        if len(current_chunk) + len(section) + 2 > max_chunk_size:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = section
        else:
            current_chunk += "\n\n" + section

    if current_chunk:
        chunks.append(current_chunk.strip())

    logger.info(f"split_content_into_chunks: created {len(chunks)} chunk(s)")
    return chunks



# --- IMAP Functions ---
def fetch_emails(since_days_ago: int = 7) -> List[Dict]:
    logger.debug("Entering fetch_emails")
    env = load_env()
    imap_server = env["IMAP_SERVER"]
    imap_user = env["IMAP_USERNAME"]
    imap_password = env["IMAP_PASSWORD"]

    date_since = (datetime.now() - timedelta(days=since_days_ago)).strftime("%d-%b-%Y")

    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(imap_user, imap_password)
    mail.select("inbox")

    result, data = mail.search(None, f'(UNSEEN SINCE {date_since})')
    email_uids = data[0].split()

    emails = []

    for uid in email_uids:
        result, data = mail.fetch(uid, "(RFC822)")
        if result != "OK":
            continue


        raw_email = email.message_from_bytes(data[0][1], policy=policy.default)

        subject = raw_email["subject"]
        from_addr = raw_email["from"]

        content = choose_best_content_from_email(raw_email)
        emails.append({
            "uid": uid.decode(),
            "subject": subject,
            "from_addr": from_addr,
            "content": content,
            "raw_email": raw_email
        })

    mail.logout()
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
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {env['XAI_API_KEY']}",
        }

        payload = {
            "model": env.get("MODEL", GROK_MODEL),
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": content}
            ],
            "temperature": 0.2,
        }

        logger.info(f"📡 Sending POST request to Grok API for process {process_id}")
        async with session.post(GROK_API_URL, headers=headers, json=payload, timeout=95) as response:
            if response.status == 200:
                data = await response.json()
                logger.info(f"✅ Grok API HTTP status: {response.status}")
                return data.get("choices", [{}])[0].get("message", {}).get("content", "")
            else:
                logger.error(f"❌ Grok API call failed with status {response.status}")
                return None
    except asyncio.TimeoutError:
        logger.error(f"⏰ Grok API call timed out for process {process_id}")
        return None
    except Exception as e:
        logger.error(f"💥 Exception during Grok API call: {e}")
        return None

    # --- Retry Wrapper ---


async def call_grok_api_with_retry(
        prompt: str,
        content: str,
        env: Dict[str, str],
        session: aiohttp.ClientSession,
        process_id: str,
        max_retries: int = 3,
        delay_seconds: int = 10
) -> Optional[str]:
    for attempt in range(1, max_retries + 1):
        logger.info(f"🔁 Grok call attempt {attempt}/{max_retries} for process {process_id}")
        result = await call_grok_api(prompt, content, env, session, process_id)
        if result:
            return result
        if attempt < max_retries:
            logger.warning(f"⏳ Retrying in {delay_seconds} seconds...")
            await asyncio.sleep(delay_seconds)

    logger.error(f"❌ All retry attempts failed for process {process_id}")
    return None


# --- Processing Functions ---
async def process_email_with_delay(
    email: Dict[str, str],
    env: Dict[str, str],
    process_id: str,
    session: aiohttp.ClientSession
) -> Tuple[List[Dict], List[Dict], Optional[Dict]]:

    logger.debug(f"Entering process_email_with_delay for UID {email.get('uid', '?')} in process {process_id}")
    logger.debug(f"📨 Raw email content for UID {email.get('uid')} (first 500 chars):\n{email.get('content', '')[:500]}")
    valid_rows = []
    skipped_rows = []
    failed_email = None

    try:
        raw_body = email.get("content", "")
        if not raw_body:
            raise ValueError("Empty email content")

        content = clean_email_content(raw_body)
        logger.info(f"Email content length after cleaning: {len(content)} characters")
        chunks = split_content_into_chunks(content, max_chunk_size=6000)
        logger.info(f"Split content into {len(chunks)} chunks")

        mappings = load_mappings("mappings.xlsx")
        domain_to_supplier = {k.strip().lower(): v.strip() for k, v in mappings.get("domain_to_supplier", {}).items()}

        logger.debug(f"domain_to_supplier mapping loaded with {len(domain_to_supplier)} entries")
        logger.debug(f"Example entries: {list(domain_to_supplier.items())[:5]}")

        content_lower = content.lower()
        subject_lower = email.get("subject", "").lower()
        is_opis = ("opis" in content_lower and ("rack" in content_lower or "wholesale" in content_lower)) or \
                  ("opis" in subject_lower and ("rack" in subject_lower or "wholesale" in subject_lower))

        prompt_file = "opis_chat_prompt.txt" if is_opis else "supplier_chat_prompt.txt"
        prompt_chat = load_prompt(prompt_file)

        if not is_opis:
            prompt_chat += "\n\n" + supply_examples_prompt_block(mappings.get("position_holders", {}))
            prompt_chat += "\n\n" + supply_lookup_prompt_block(mappings.get("supply_lookup", {}))
            prompt_chat += "\n\n" + terminal_mapping_prompt_block(mappings.get("terminals", {}))

        email_from = email.get("from_addr", "")
        supplier = None

        if email_from:
            match = re.search(r"[\w\.-]+@([\w\.-]+)", email_from)
            if match:
                domain = match.group(1).strip().lower()
                logger.info(f"Parsed domain from from_addr: {domain}")
                supplier = domain_to_supplier.get(domain)
                if not supplier:
                    logger.warning(f"Domain '{domain}' not found in domain_to_supplier. Attempting fuzzy match...")
                    for known_domain, known_supplier in domain_to_supplier.items():
                        if domain.endswith(known_domain):
                            supplier = known_supplier
                            logger.info(f"Fuzzy matched supplier '{supplier}' for domain '{domain}' using known '{known_domain}'")
                            break

        if not supplier:
            logger.debug("Trying extract_domain_from_forwarded_headers()...")
            supplier = extract_domain_from_forwarded_headers(raw_body, domain_to_supplier)

        if not supplier:
            logger.debug("Trying extract_domains_from_body()...")
            supplier = extract_domains_from_body(raw_body, domain_to_supplier)

        if not supplier:
            supplier = "Unknown Supplier"
            logger.warning(f"No supplier identified for UID {email.get('uid', '?')}, defaulting to Unknown Supplier")

        parsed_rows = []
        for idx, chunk in enumerate(chunks):
            logger.info(f"Calling Grok API for chunk {idx+1}/{len(chunks)} for UID {email.get('uid', '?')}")
            logger.debug(f"Prompt being sent to Grok for UID {email.get('uid')}:\n{prompt_chat}\n---\nChunk:\n{chunk}")
            parsed = await call_grok_api_with_retry(prompt_chat, chunk, env, session, process_id)

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

            if not is_opis and not row.get("Supplier"):
                row["Supplier"] = supplier

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


from typing import Optional

#DELAY DELETION OF STATUS
def schedule_delayed_status_deletion(process_id: str, delay_seconds: int = 300):
    async def delete_later():
        await asyncio.sleep(delay_seconds)
        delete_process_status(process_id)
    asyncio.create_task(delete_later())

async def process_all_emails(process_id: str, process_statuses: Dict[str, dict], model: Optional[str] = None) -> None:
    logger.info(f"Parser.py version: 2025-05-02 with model selection, dynamic prompt injection, supply mapping, and full logging")
    file_handler = setup_file_logging(process_id)

    try:
        logger.debug("Loading environment variables")
        env = load_env()

        if model:
            env["MODEL"] = model
            logger.info(f"🧠 Overriding MODEL for this run to: {model}")

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

        logger.debug("Fetching emails")
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

        logger.debug("Creating aiohttp ClientSession")
        async with aiohttp.ClientSession() as session:
            for idx, email in enumerate(emails):
                process_statuses[process_id]["current_email"] = idx + 1
                save_process_status(process_id, process_statuses[process_id])
                logger.info(f"Processing email {idx + 1}/{len(emails)} for process {process_id}")
                logger.debug(f"Email details: {email}")

                valid_rows, skipped_rows, failed_email = await process_email_with_delay(email, env, process_id, session)

                if valid_rows:
                    save_to_csv(valid_rows, output_file, process_id)
                    total_rows += len(valid_rows)
                    process_statuses[process_id]["row_count"] = total_rows
                    save_process_status(process_id, process_statuses[process_id])

                if failed_email:
                    failed_email["content"] = email.get("content", "")
                    failed_emails.append(failed_email)

                await asyncio.sleep(5)

        if failed_emails:
            save_failed_emails_to_csv(failed_emails, output_file, process_id)

        process_statuses[process_id]["status"] = "done"
        process_statuses[process_id]["output_file"] = output_file
        save_process_status(process_id, process_statuses[process_id])
        logger.info(f"Completed process {process_id} with {total_rows} rows")

        schedule_delayed_status_deletion(process_id, delay_seconds=300)

    except Exception as e:
        logger.error(f"Error in process_all_emails for process {process_id}: {str(e)}")
        process_statuses[process_id]["status"] = "error"
        process_statuses[process_id]["error"] = str(e)
        save_process_status(process_id, process_statuses[process_id])

    finally:
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