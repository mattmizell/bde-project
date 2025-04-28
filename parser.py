# parser.py (Updated with Fixes)

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

# Configure logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("parser")

# Output and Prompts directory
BASE_DIR: Path = Path(__file__).parent
OUTPUT_DIR: Path = BASE_DIR / "output"
PROMPTS_DIR: Path = BASE_DIR / "prompts"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Global mappings
SUPPLIER_MAPPING: Dict[str, str] = {}
PRODUCT_MAPPING: Dict[str, str] = {}
TERMINAL_MAPPING: List[Dict[str, str]] = []

# Expected price range for validation
EXPECTED_PRICE_RANGE = {"E10": (1.50, 4.00), "ULSD": (1.80, 3.50)}  # Adjust based on historical data
PRICE_TOLERANCE = 0.01  # Tolerance for price discrepancies
REQUIRED_PRODUCTS = ["ULSD", "87E10"]  # Products expected at each terminal


# --- Setup Functions ---

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


def apply_translations(row: Dict, supplier_mapping: Dict, product_mapping: Dict, terminal_mapping: List[Dict]) -> Dict:
    # Apply Supplier mapping
    row["Supplier"] = supplier_mapping.get(row.get("Supplier", ""), row.get("Supplier", ""))
    # Apply Supply mapping (if Supply is null, it defaults to Supplier in the prompt, so we map it here)
    row["Supply"] = supplier_mapping.get(row.get("Supply", ""), row.get("Supply", ""))

    # Apply Product mapping
    row["Product Name"] = product_mapping.get(row.get("Product Name", ""), row.get("Product Name", ""))

    # Apply Terminal mapping with condition handling
    terminal = row.get("Terminal", "")
    for term_map in terminal_mapping:
        if term_map["raw_value"] == terminal:
            if term_map["condition"]:
                if "Supplier in" in term_map["condition"]:
                    allowed_suppliers = eval(term_map["condition"].split("in")[1].strip())
                    if row["Supplier"] in allowed_suppliers:
                        row["Terminal"] = term_map["standardized_value"]
                        break
                elif "Supplier not in" in term_map["condition"]:
                    disallowed_suppliers = eval(term_map["condition"].split("in")[1].strip())
                    if row["Supplier"] not in disallowed_suppliers:
                        row["Terminal"] = term_map["standardized_value"]
                        break
            else:
                row["Terminal"] = term_map["standardized_value"]
                break
    return row


# --- Preprocessing Functions ---

def preprocess_content(content: str) -> str:
    """
    Preprocess the content to handle truncation and special characters.
    """
    # Handle truncation by ensuring the last line ends properly
    if not content.endswith('\n'):
        content += '"\n'

    # Replace special characters that might break JSON parsing
    content = content.replace('-- --', 'N/A')
    content = content.replace('+++', 'N/A')
    return content


def extract_date(content: str, default_date="2025-04-26", default_time="00:01") -> Tuple[str, str]:
    """
    Extract the effective date and time from the content, particularly for OPIS files.
    """
    date_match = re.search(r'(\d{2}/\d{2}/\d{2}) (\d{2}:\d{2}:\d{2}) EDT', content)
    if date_match:
        date_str, time_str = date_match.groups()
        date = f"2025-{date_str[0:2]}-{date_str[3:5]}"
        return date, time_str[:5]  # e.g., "2025-04-26", "10:01"
    return default_date, default_time


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
    prompt_path = PROMPTS_DIR / filename
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()


async def call_grok_api(prompt: str, content: str, env: Dict[str, str], session: aiohttp.ClientSession) -> Optional[
    str]:
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


# --- Data Validation and Correction Functions ---

def deduplicate_rows(rows: List[Dict]) -> List[Dict]:
    """
    Remove duplicate entries based on Supplier, Terminal, and Product Name.
    """
    seen = set()
    deduplicated = []
    for row in rows:
        key = (row["Supplier"], row["Terminal"], row["Product Name"])
        if key not in seen:
            seen.add(key)
            deduplicated.append(row)
        else:
            logger.warning(f"Duplicate entry removed: {key}")
    return deduplicated


def validate_prices(rows: List[Dict]) -> List[Dict]:
    """
    Validate prices and flag outliers.
    """
    for row in rows:
        product = row["Product Name"]
        price = row["Price"]
        price_range = None
        if "E10" in product:
            price_range = EXPECTED_PRICE_RANGE.get("E10")
        elif "ULSD" in product:
            price_range = EXPECTED_PRICE_RANGE.get("ULSD")

        if price_range and (price < price_range[0] or price > price_range[1]):
            logger.warning(f"Outlier price detected: {product} at {row['Terminal']} - Price: {price}")
            row["Price"] = None  # Set to None for downstream handling
    return rows


def handle_price_discrepancies(rows: List[Dict], tolerance: float = PRICE_TOLERANCE) -> List[Dict]:
    """
    Handle price discrepancies for the same product and terminal across suppliers.
    """
    grouped_by_key = {}
    for row in rows:
        key = (row["Terminal"], row["Product Name"])
        grouped_by_key.setdefault(key, []).append(row)

    for key, entries in grouped_by_key.items():
        if len(entries) > 1:  # Multiple suppliers for the same product/terminal
            prices = [e["Price"] for e in entries if e["Price"] is not None]
            if prices and max(prices) - min(prices) <= tolerance:
                avg_price = sum(prices) / len(prices)
                for entry in entries:
                    entry["Price"] = avg_price
            elif prices:
                logger.warning(f"Price discrepancy exceeds tolerance at {key}: {prices}")
    return rows


def ensure_required_products(rows: List[Dict], required_products: List[str] = REQUIRED_PRODUCTS) -> List[Dict]:
    """
    Ensure all required products are present for each terminal, adding placeholders if missing.
    """
    grouped_by_terminal = {}
    for row in rows:
        key = (row["Supplier"], row["Terminal"])
        grouped_by_terminal.setdefault(key, []).append(row["Product Name"])

    for (supplier, terminal), products in grouped_by_terminal.items():
        for required in required_products:
            if required not in products:
                rows.append({
                    "Supplier": supplier,
                    "Supply": supplier,
                    "Product Name": required,
                    "Terminal": terminal,
                    "Price": None,
                    "Volume Type": "Contract",
                    "Effective Date": rows[0]["Effective Date"],
                    "Effective Time": rows[0]["Effective Time"],
                })
                logger.warning(f"Added placeholder for missing product {required} at {terminal} for {supplier}")
    return rows


# --- Processing Functions ---

async def process_email_with_delay(email: Dict[str, str], env: Dict[str, str], process_id: str,
                                   session: aiohttp.ClientSession) -> Tuple[List[Dict], List[Dict], Optional[Dict]]:
    valid_rows, skipped_rows, failed_email = [], [], None
    try:
        content = clean_email_content(email.get("content", ""))
        if not content:
            raise ValueError("Empty email content")

        # Preprocess content to handle truncation and special characters
        content = preprocess_content(content)

        # Extract date (important for OPIS)
        effective_date, effective_time = extract_date(content)

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

        # Safely parse JSON to handle potential unterminated strings
        try:
            rows = json.loads(parsed)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            # Attempt to fix by truncating at the last valid JSON object
            last_valid = parsed[:e.pos]
            rows = json.loads(last_valid + ']}')  # Close the JSON array

        logger.debug(f"Parsed {len(rows)} rows from email UID {email.get('uid', '?')}")
        for row in rows:
            # Apply mappings to standardize the row
            row = apply_translations(row, SUPPLIER_MAPPING, PRODUCT_MAPPING, TERMINAL_MAPPING)
            # Override date/time with extracted values
            row["Effective Date"] = effective_date
            row["Effective Time"] = effective_time
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

        # Post-process the rows
        valid_rows = deduplicate_rows(valid_rows)  # Remove duplicates
        valid_rows = validate_prices(valid_rows)  # Flag outliers
        valid_rows = handle_price_discrepancies(valid_rows)  # Resolve price discrepancies
        valid_rows = ensure_required_products(valid_rows)  # Add placeholders for missing products

        if valid_rows:
            mark_email_as_processed(email.get("uid", ""), env)

    except Exception as ex:
        failed_email = {"email_id": email.get("uid", "?"), "subject": email.get("subject", ""),
                        "from_addr": email.get("from_addr", ""), "error": str(ex)}
        logger.error(f"Failed to process email UID {email.get('uid', '?')}: {str(ex)}")
    return valid_rows, skipped_rows, failed_email


async def process_all_emails(process_id: str, process_statuses: Dict[str, dict]) -> None:
    env = load_env()
    emails = fetch_emails(env, process_id)
    process_statuses[process_id]["email_count"] = len(emails)
    if not emails:
        process_statuses[process_id]["status"] = "done"
        return

    output_file = f"parsed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    total_rows = 0
    failed_emails = []

    async with aiohttp.ClientSession() as session:
        for idx, email in enumerate(emails):
            process_statuses[process_id]["current_email"] = idx + 1
            valid_rows, skipped_rows, failed_email = await process_email_with_delay(email, env, process_id, session)

            if valid_rows:
                save_to_csv(valid_rows, output_file, process_id)
                total_rows += len(valid_rows)
                process_statuses[process_id]["row_count"] = total_rows

            if failed_email:
                failed_email["content"] = email.get("content", "")
                failed_emails.append(failed_email)

            await asyncio.sleep(2)

    if failed_emails:
        save_failed_emails_to_csv(failed_emails, output_file, process_id)

    process_statuses[process_id]["status"] = "done"
    process_statuses[process_id]["output_file"] = output_file


# --- CSV Saving Functions ---

def save_to_csv(data: List[Dict], output_filename: str, process_id: str) -> None:
    try:
        output_path = OUTPUT_DIR / output_filename
        fieldnames = ["Supplier", "Supply", "Product Name", "Terminal", "Price", "Volume Type", "Effective Date",
                      "Effective Time"]
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