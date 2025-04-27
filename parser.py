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

# Output directory
BASE_DIR: Path = Path(__file__).parent
OUTPUT_DIR: Path = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Global mappings
SUPPLIER_MAPPING: Dict[str, str] = {}
PRODUCT_MAPPING: Dict[str, str] = {}
TERMINAL_MAPPING: List[Dict[str, str]] = []


def load_env() -> Dict[str, str]:
    logger.info("Loading environment variables...")
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
            logger.error(f"Missing required environment variable: {key}")
            raise ValueError(f"Missing required environment variable: {key}")
    logger.info("Environment variables loaded successfully")
    return env_vars


def initialize_mappings() -> None:
    global SUPPLIER_MAPPING, PRODUCT_MAPPING, TERMINAL_MAPPING
    logger.info("Initializing mappings...")
    mappings_file = BASE_DIR / "mappings.xlsx"
    if not mappings_file.exists():
        raise FileNotFoundError(f"Mappings file not found: {mappings_file}")

    df_suppliers = pd.read_excel(mappings_file, sheet_name="SupplierMappings")
    SUPPLIER_MAPPING = dict(zip(df_suppliers["Raw Value"], df_suppliers["Standardized Value"]))
    logger.info(f"Loaded {len(SUPPLIER_MAPPING)} supplier mappings")

    df_products = pd.read_excel(mappings_file, sheet_name="ProductMappings")
    PRODUCT_MAPPING = dict(zip(df_products["Raw Value"], df_products["Standardized Value"]))
    logger.info(f"Loaded {len(PRODUCT_MAPPING)} product mappings")

    df_terminals = pd.read_excel(mappings_file, sheet_name="TerminalMappings")
    TERMINAL_MAPPING = []
    for _, row in df_terminals.iterrows():
        TERMINAL_MAPPING.append({
            "raw_value": str(row["Raw Value"]),
            "standardized_value": str(row["Standardized Value"]),
            "condition": str(row["Condition"]) if "Condition" in row and pd.notna(row["Condition"]) else None
        })
    logger.info(f"Loaded {len(TERMINAL_MAPPING)} terminal mappings")


def choose_best_content_from_email(msg) -> str:
    """
    If the email has a .txt attachment, use it. Otherwise, use the plain text body.
    """
    for part in msg.walk():
        filename = part.get_filename()
        if filename and filename.endswith(".txt"):
            try:
                attachment_content = part.get_payload(decode=True).decode(errors="ignore")
                if attachment_content.strip():
                    logger.info(f"Using .txt attachment: {filename}")
                    return attachment_content
            except Exception as e:
                logger.error(f"Failed to decode attachment {filename}: {e}")

    body = msg.get_body(preferencelist=("plain"))
    if body:
        return body.get_content().strip()

    logger.warning("No valid body or attachment found.")
    return ""


async def fetch_emails(env: Dict[str, str], process_id: str) -> List[Dict[str, str]]:
    logger.info(f"Process {process_id}: Fetching emails")
    try:
        imap_server = imaplib.IMAP4_SSL(env["IMAP_SERVER"])
        imap_server.login(env["IMAP_USERNAME"], env["IMAP_PASSWORD"])
        imap_server.select("INBOX")
        since_date = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
        logger.info(f"Searching for emails since {since_date}")
        _, msg_nums = imap_server.search(None, f'(SINCE "{since_date}") UNSEEN')

        emails = []
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
        logger.info(f"Fetched {len(emails)} emails")
        return emails
    except Exception as ex:
        logger.error(f"Failed to fetch emails: {str(ex)}")
        return []


def clean_email_content(content: str) -> str:
    try:
        content = content.replace("=\n", "")
        content = re.sub(r"-{40,}", "", content)
        content = re.sub(r"\n{3,}", "\n\n", content)
        cleaned = "\n".join(line.strip() for line in content.splitlines())
        return cleaned[:6000].strip()
    except Exception as e:
        logger.error(f"Content cleaning failed: {e}")
        return content.strip()


def mark_email_as_processed(uid: str, env: Dict[str, str]) -> None:
    """
    Mark an email as processed by adding a Gmail label 'BDE_Processed'.
    """
    try:
        imap_server = imaplib.IMAP4_SSL(env["IMAP_SERVER"])
        imap_server.login(env["IMAP_USERNAME"], env["IMAP_PASSWORD"])
        imap_server.select("INBOX")
        imap_server.store(uid, '+X-GM-LABELS', 'BDE_Processed')
        logger.info(f"Marked email UID {uid} as processed (BDE_Processed)")
        imap_server.logout()
    except Exception as ex:
        logger.error(f"Failed to mark email UID {uid} as processed: {str(ex)}")


async def process_email_with_delay(
    email: Dict[str, str],
    env: Dict[str, str],
    process_id: str,
    session: aiohttp.ClientSession
) -> Tuple[List[Dict], List[Dict], Optional[Dict]]:
    valid_rows = []
    skipped_rows = []
    failed_email = None

    try:
        email_content = clean_email_content(email.get("content", ""))
        if not email_content:
            raise ValueError("No content found in email")

        is_opis = (
            "OPIS" in email_content and
            ("Rack" in email_content or "Wholesale" in email_content) and
            "Effective Date" in email_content
        )

        if is_opis:
            logger.info(f"Detected OPIS rack report for {email.get('uid', '?')}")
            prompt = (
                "You are an expert data extractor for OPIS Rack Pricing Reports.\n\n"
                "Extract the following fields **exactly as described** for each product listed:\n"
                "- Supplier: Supplier or Position Holder (if both shown, prefer Supplier)\n"
                "- Supply: Same as Supplier unless a different Position Holder is clearly stated\n"
                "- Product Name: The full product name exactly as listed\n"
                "- Terminal: Terminal or City name listed in section headers\n"
                "- Price: Rack Average price (choose Rack Avg. If missing, fallback to Spot Mean)\n"
                "- Volume Type: Always set to 'Contract'\n"
                "- Effective Date: Date the prices apply, in YYYY-MM-DD format\n"
                "- Effective Time: Always set to '00:01' unless otherwise indicated\n\n"
                "⚡ Important Rules:\n"
                "- Always output pure JSON array without any extra text.\n"
                "- If Supplier or Terminal applies to multiple rows, inherit it.\n"
                "- If a field is missing, set it as null (not empty string).\n"
                "- Do not guess values. Only use clearly visible data.\n"
                "- Prioritize 'Rack Avg' prices. Only use 'Spot Mean' if Rack Avg missing.\n"
                "- If product and price are not clear, skip that row.\n"
                "- Preserve accurate decimals in prices.\n\n"
                "Here is the OPIS rack report:\n\n"
                f"{email_content}"
            )
        else:
            logger.info(f"Detected Supplier pricing email for {email.get('uid', '?')}")
            prompt = (
                "You are an expert at extracting pricing information from complex petroleum supplier emails for Better Day Energy.\n\n"
                "Extract the following fields:\n"
                "- Supplier\n- Supply\n- Product Name\n- Terminal\n"
                "- Price (numeric)\n- Volume Type\n- Effective Date\n- Effective Time\n\n"
                "⚡ Output pure JSON array only.\n"
                "⚡ Missing fields must be set to null.\n"
                "⚡ Split Supply/Terminal if combined.\n"
                "⚡ No assumptions — only based on text.\n\n"
                "Email Content:\n\n"
                f"{email_content}"
            )

        api_url = "https://api.x.ai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {env['XAI_API_KEY']}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": env.get("MODEL", "grok-3-latest"),
            "messages": [{"role": "user", "content": prompt}],
        }

        async with session.post(api_url, headers=headers, json=payload) as response:
            raw_text = await response.text()

        logger.debug(f"Grok API response: {raw_text}")
        data = json.loads(raw_text)
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "[]")

        if content.startswith("```json"):
            match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
            if match:
                content = match.group(1).strip()

        parsed_data = json.loads(content)

        for row in parsed_data:
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

        # ✅ Mark email as processed only if parsing succeeded
        if valid_rows:
            mark_email_as_processed(email.get("uid", ""), env)

    except Exception as ex:
        logger.error(f"Failed to process email {email.get('uid', '?')}: {str(ex)}")
        failed_email = {
            "email_id": email.get("uid", "?"),
            "subject": email.get("subject", ""),
            "from_addr": email.get("from_addr", ""),
            "error": str(ex),
        }

    return valid_rows, skipped_rows, failed_email


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
        logger.info(f"Saved {len(data)} rows to {output_path}")
    except Exception as ex:
        logger.error(f"Failed to save CSV: {ex}")


def save_failed_emails_to_csv(failed_emails: List[Dict], output_filename: str, process_id: str) -> None:
    try:
        if not failed_emails:
            return
        failed_filename = f"failed_{output_filename.split('_')[1]}"
        failed_path = OUTPUT_DIR / failed_filename
        df_failed = pd.DataFrame(failed_emails)
        df_failed.to_csv(failed_path, index=False)
        logger.info(f"Saved failed emails to {failed_path}")
    except Exception as ex:
        logger.error(f"Failed to save failed emails to CSV: {ex}")
