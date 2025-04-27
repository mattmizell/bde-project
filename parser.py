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
PROMPTS_DIR: Path = BASE_DIR / "prompts"
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

def load_prompt(filename: str) -> str:
    prompt_path = PROMPTS_DIR / filename
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()

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
    try:
        imap_server = imaplib.IMAP4_SSL(env["IMAP_SERVER"])
        imap_server.login(env["IMAP_USERNAME"], env["IMAP_PASSWORD"])
        imap_server.select("INBOX")
        imap_server.store(uid, '+X-GM-LABELS', 'BDE_Processed')
        logger.info(f"Marked email UID {uid} as processed (BDE_Processed)")
        imap_server.logout()
    except Exception as ex:
        logger.error(f"Failed to mark email UID {uid} as processed: {str(ex)}")

async def call_grok_api(prompt: str, content: str, env: Dict[str, str], session: aiohttp.ClientSession, use_parse: bool) -> Optional[str]:
    try:
        api_url = "https://api.x.ai/v1/parse" if use_parse else "https://api.x.ai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {env['XAI_API_KEY']}",
            "Content-Type": "application/json",
        }
        if use_parse:
            payload = {"document": content, "instructions": prompt}
        else:
            payload = {
                "model": env.get("MODEL", "grok-3-latest"),
                "messages": [{"role": "system", "content": prompt}, {"role": "user", "content": content}],
            }

        async with session.post(api_url, headers=headers, json=payload) as response:
            response.raise_for_status()
            raw_text = await response.text()

        logger.debug(f"Grok API response: {raw_text}")
        data = json.loads(raw_text)
        if use_parse:
            return json.dumps(data.get("data", []))
        else:
            return data.get("choices", [{}])[0].get("message", {}).get("content", "[]")

    except Exception as ex:
        logger.error(f"Grok API call failed: {ex}")
        return None

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

        prompt_file_parse = "opis_parse_prompt.txt" if is_opis else "supplier_parse_prompt.txt"
        prompt_file_chat = "opis_chat_prompt.txt" if is_opis else "supplier_chat_prompt.txt"

        prompt_parse = load_prompt(prompt_file_parse)
        prompt_chat = load_prompt(prompt_file_chat)

        # Try Parse API first
        content = await call_grok_api(prompt_parse, email_content, env, session, use_parse=True)
        if not content:
            # Fallback to Chat API
            content = await call_grok_api(prompt_chat, email_content, env, session, use_parse=False)

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