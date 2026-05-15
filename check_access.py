import asyncio
import aiohttp
import time
import logging
from aiohttp import TCPConnector

CONCURRENT_REQUESTS = 50
CONNECTION_LIMIT = 100
BATCH_SIZE = 500

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def check_url(session, url, result_file, blocked_file):
    try:
        async with session.get(url, timeout=10, allow_redirects=True) as response:
            if response.status in (200, 301, 302, 303, 307, 308):
                with open(result_file, 'a', encoding='utf-8') as f:
                    f.write(f"{url}\n")
                logging.info(f"ACCESSIBLE: {url}")
            else:
                with open(blocked_file, 'a', encoding='utf-8') as f:
                    f.write(f"{url} (status: {response.status})\n")
                logging.info(f"BLOCKED: {url} (status: {response.status})")
    except asyncio.TimeoutError:
        with open(blocked_file, 'a', encoding='utf-8') as f:
            f.write(f"{url} (timeout)\n")
        logging.info(f"TIMEOUT: {url}")
    except Exception as e:
        with open(blocked_file, 'a', encoding='utf-8') as f:
            f.write(f"{url} (error: {str(e)})\n")
        logging.info(f"ERROR: {url} - {e}")

async def bound_fetch(sem, session, url, result_file, blocked_file):
    async with sem:
        await check_url(session, url, result_file, blocked_file)

def clean_url(url):
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    return url

async def process_urls(urls, result_file, blocked_file):
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = TCPConnector(limit=CONNECTION_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for url in urls:
            cleaned_url = clean_url(url)
            if cleaned_url:
                task = bound_fetch(sem, session, cleaned_url, result_file, blocked_file)
                tasks.append(task)
        await asyncio.gather(*tasks)

async def process_file_in_batches(input_file, result_file, blocked_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        batch = []
        for line in file:
            batch.append(line.strip())
            if len(batch) >= BATCH_SIZE:
                await process_urls(batch, result_file, blocked_file)
                batch = []
        if batch:
            await process_urls(batch, result_file, blocked_file)

def main():
    input_file = 'kr.list'
    result_file = 'accessible.txt'
    blocked_file = 'blocked.txt'

    open(result_file, 'w').close()
    open(blocked_file, 'w').close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_file_in_batches(input_file, result_file, blocked_file))

if __name__ == '__main__':
    start_time = time.time()
    main()
    print(f"Completed in {time.time() - start_time} seconds")