# parserpy
import os
from dotenv import load_dotenv
load_dotenv()

PORT = int(os.getenv("PORT", 8000))

import os
import imaplib
import aiohttp
import asyncio
import logging
import pandas as pd
import json
import re
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import csv
from pathlib import Path
from dotenv import load_dotenv

# CORRECT email parsing imports
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


async def fetch_emails(env: Dict[str, str], process_id: str) -> List[Dict[str, str]]:
    logger.info(f"Process {process_id}: Fetching emails")
    try:
        imap_server = imaplib.IMAP4_SSL(env["IMAP_SERVER"])
        imap_server.login(env["IMAP_USERNAME"], env["IMAP_PASSWORD"])
        imap_server.select("INBOX")
        since_date = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
        logger.info(f"Process {process_id}: Searching for emails since {since_date}")
        _, msg_nums = imap_server.search(None, f'(SINCE "{since_date}") UNSEEN')
        logger.info(f"Process {process_id}: Found {len(msg_nums[0].split())} unread emails")

        emails = []
        for num in msg_nums[0].split():
            logger.info(f"Process {process_id}: Fetching email {num.decode()}")
            _, data = imap_server.fetch(num, "(RFC822)")
            msg = BytesParser(policy=policy.default).parsebytes(data[0][1])
            content_part = msg.get_body(preferencelist="plain")
            content = content_part.get_content() if content_part else ""
            for part in msg.walk():
                if part.get_filename() and part.get_filename().endswith(".txt"):
                    content += part.get_payload(decode=True).decode(errors="ignore")
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
        logger.info(f"Process {process_id}: Fetched {len(emails)} emails")
        return emails
    except Exception as ex:
        logger.error(f"Process {process_id}: Failed to fetch emails: {str(ex)}")
        return []


def clean_email_content(content: str) -> str:
    try:
        content = re.sub(r"-{40,}", "", content)
        content = re.sub(r"\n{3,}", "\n\n", content)
        cleaned = "\n".join(line.strip() for line in content.split("\n") if line.strip())
        return cleaned[:6000].strip()
    except Exception as e:
        logger.error(f"Content cleaning failed: {e}")
        return content.strip()


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

        # Build the prompt
        prompt = (
            "You are an expert at extracting fuel pricing information from emails.\n\n"
            "For each product, extract the following fields:\n"
            "- Supplier: (The marketing company sending the email: Wallis, Luke Oil, WFS, Bylo, OPIS)\n"
            "- Supply: (The position holder: a true fuel company like Marathon, Shell, Exxon, BP, Cenex, PSX, XOM)\n"
            "- Product Name: (e.g., 87E10, ULSD, CBOB)\n"
            "- Terminal: (the city or location terminal, separate from supply)\n"
            "- Price: (a number)\n"
            "- Volume Type: (spot, contract, rack, etc.)\n"
            "- Effective Date: (YYYY-MM-DD)\n"
            "- Effective Time: (24-hour format HH:MM)\n\n"
            "⚡ IMPORTANT:\n"
            "- If supply and terminal are combined (e.g., 'PSX Cahokia'), split them:\n"
            "   - Supply = 'PSX'\n"
            "   - Terminal = 'Cahokia'\n"
            "- Supply MUST be a known refiner like Marathon, Shell, BP, PSX, XOM, etc.\n"
            "- If missing, guess based on common oil company names.\n"
            "- If you cannot find supply, set Supply = null.\n"
            "- Output pure JSON array. No text outside the array.\n"
            "- Price must be a number, not string.\n"
            "- If Effective Date missing, assume from email sent date.\n\n"
            "Here is the email content:\n\n"
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

        async with session.post(api_url, json=payload, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"API error: {response.status} {await response.text()}")

            raw_text = await response.text()
            logger.debug(f"Process {process_id}: Raw Grok API response for email {email.get('uid', '?')}:\n{raw_text}")

            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError as e:
                logger.error(f"Process {process_id}: JSON decode error on Grok response: {e}")
                raise

        content = data.get("choices", [{}])[0].get("message", {}).get("content", "[]")

        # Extract the JSON inside ```json ... ``` if it exists
        if content.startswith("```json"):
            match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
            if match:
                content = match.group(1).strip()
            else:
                raise ValueError("Failed to extract JSON block from Grok response")

        parsed_data = json.loads(content) if isinstance(content, str) else content

        for row in parsed_data:
            # Fill missing Effective Date with email "Sent Date" if available
            effective_date = row.get("Effective Date")
            if not effective_date and isinstance(email.get("date"), datetime):
                effective_date = email["date"].strftime("%Y-%m-%d")

            valid_rows.append({
                "Supplier": row.get("Supplier", ""),
                "Supply": row.get("Supply", ""),
                "Product Name": row.get("Product Name", ""),
                "Terminal": row.get("Terminal", ""),
                "Price": row.get("Price", 0),
                "Volume Type": row.get("Volume Type", ""),
                "Effective Date": effective_date or "",
                "Effective Time": row.get("Effective Time", ""),
            })

    except Exception as ex:
        logger.error(f"Process {process_id}: Failed to process email {email.get('uid', '?')}: {str(ex)}")
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
        logger.info(f"Process {process_id}: Saved {len(data)} rows to {output_path}")
    except Exception as ex:
        logger.error(f"Process {process_id}: CSV save failed: {ex}")


def save_failed_emails_to_csv(failed_emails: List[Dict], output_filename: str, process_id: str) -> None:
    try:
        if not failed_emails:
            return
        failed_filename = f"failed_{output_filename.split('_')[1]}"
        failed_path = OUTPUT_DIR / failed_filename
        df_failed = pd.DataFrame(failed_emails)
        df_failed.to_csv(failed_path, index=False)
        logger.info(f"Process {process_id}: Saved failed emails to {failed_path}")
    except Exception as ex:
        logger.error(f"Process {process_id}: Failed to save failed emails to CSV: {ex}")
