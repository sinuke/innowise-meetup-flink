import os
import re
import asyncio
import hashlib
import logging
import httpx
from datetime import datetime


BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
TARGET_DIR = r"../input_raws/full_pubmed_baseline_2026"
LOGS_SUBDIR = r"./logs"
MAX_CONCURRENT_DOWNLOADS = 5


os.makedirs(TARGET_DIR, exist_ok=True)
os.makedirs(LOGS_SUBDIR, exist_ok=True)


# --- Logging Configuration ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOGS_SUBDIR, f"pubmed_download_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def verify_md5(file_path, md5_path):
    """Compares the raw file hash against the string stored in the local md5 file."""
    try:
        with open(md5_path, 'r', encoding='utf-8') as f:
            md5_content = f.read()
        
        # Robustly extract the 32-character hex MD5 string from the file
        match = re.search(r'[a-fA-F0-9]{32}', md5_content)
        if not match:
            logging.error(f"Could not find a valid 32-character MD5 hash inside {os.path.basename(md5_path)}")
            return False
            
        expected_md5 = match.group(0).lower()
        
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                hasher.update(chunk)
        
        return hasher.hexdigest() == expected_md5
    except Exception as e:
        logging.error(f"MD5 Read/Verification Error for {os.path.basename(file_path)}: {e}")
        return False


async def download_md5(client, file_number, semaphore):
    """Phase 1: Fetch and save the tiny .md5 file text, skipping if already present."""
    md5_filename = f"pubmed26n{file_number:04d}.xml.gz.md5"
    url = f"{BASE_URL}{md5_filename}"
    file_path = os.path.join(TARGET_DIR, md5_filename)
    
    # Skip logic for Phase 1
    if os.path.exists(file_path):
        logging.info(f"Checksum file already exists: {md5_filename}. Skipping download.")
        return

    async with semaphore:
        try:
            logging.info(f"Fetching checksum: {md5_filename}")
            response = await client.get(url, timeout=15.0)
            if response.status_code == 200:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
            else:
                logging.error(f"Server returned status {response.status_code} for {md5_filename}")
        except Exception as e:
            logging.error(f"FAILED fetching checksum {md5_filename}: {e}")


async def download_archive(client, file_number, semaphore):
    """Phase 2: Stream download the archive if missing, or validate locally if it exists."""
    filename = f"pubmed26n{file_number:04d}.xml.gz"
    url = f"{BASE_URL}{filename}"
    file_path = os.path.join(TARGET_DIR, filename)
    md5_path = os.path.join(TARGET_DIR, f"{filename}.md5")
    
    if not os.path.exists(md5_path):
        logging.error(f"Skipping archive {filename} because local matching MD5 file is missing.")
        return

    # Skip download logic for Phase 2 (still runs a local integrity check)
    if os.path.exists(file_path):
        logging.info(f"Archive already exists: {filename}. Skipping download. Validating local file...")
        if verify_md5(file_path, md5_path):
            logging.info(f"SUCCESS: Existing file {filename} verified perfectly against local MD5.")
            return
        else:
            logging.critical(f"MISMATCH: Existing file {filename} failed MD5 validation! Redownload.")

    async with semaphore:
        try:
            logging.info(f"Starting stream download: {filename}")
            async with client.stream("GET", url, timeout=60.0) as response:
                if response.status_code == 200:
                    with open(file_path, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            f.write(chunk)
                    
                    # Verify immediately after downloading
                    if verify_md5(file_path, md5_path):
                        logging.info(f"SUCCESS: {filename} downloaded and verified perfectly.")
                    else:
                        logging.critical(f"MISMATCH: Newly downloaded {filename} failed MD5 validation! Keeping file on disk.")
                else:
                    logging.error(f"Failed downloading {filename}: Status {response.status_code}")
        except Exception as e:
            logging.error(f"FAILED processing archive {filename} -> {e}")


async def main():
    logging.info("="*60)
    logging.info("Starting Async PubMed 2026 Baseline Download Session via httpx")
    logging.info(f"Destination: {TARGET_DIR}")
    logging.info("="*60)
    
    file_numbers = list(range(1, 1335))  # Full Baseline: 0001 to 1334
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    limits = httpx.Limits(max_keepalive_connections=MAX_CONCURRENT_DOWNLOADS, max_connections=20)
    
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        logging.info("--- Checking/Downloading Checksum (.md5) Files ---")
        md5_tasks = [download_md5(client, num, semaphore) for num in file_numbers]
        await asyncio.gather(*md5_tasks)

        logging.info("--- Checking/Downloading XML Archives ---")
        archive_tasks = [download_archive(client, num, semaphore) for num in file_numbers]
        await asyncio.gather(*archive_tasks)
        
    logging.info("="*60)
    logging.info("PubMed Baseline Download Session Finished completely.")
    logging.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
