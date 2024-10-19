import orjson
from typing import List, Tuple
from datetime import datetime
from collections import Counter, defaultdict
import heapq
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import mmap
from config import JSON_FILENAME
from JsonMaker import JsonMaker

def process_chunk(chunk: bytes) -> Tuple[Counter, defaultdict]:
    date_tweet_count = Counter()
    date_user_count = defaultdict(Counter)
    
    for line in chunk.splitlines():
        if not line.strip():
            continue
        try:
            tweet = orjson.loads(line)
            date_str = tweet['date'][:10]
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
            username = tweet['user']['username']
            
            date_tweet_count[date] += 1
            date_user_count[date][username] += 1
        except (orjson.JSONDecodeError, KeyError):
            # Salta json invalidos
            continue
    
    return date_tweet_count, date_user_count

def read_in_chunks(mm, chunk_size=1024*1024):
    """Generator to read mmap object in chunks."""
    current = 0
    while current < len(mm):
        chunk = mm[current:current+chunk_size]
        yield chunk
        current += chunk_size

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    num_processes = os.cpu_count() or 1
    chunk_size = 10 * 1024 * 1024  # 10MB chunks
    
    with open(file_path, 'rb') as file:
        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            futures = [executor.submit(process_chunk, chunk) for chunk in read_in_chunks(mm, chunk_size)]
            
            date_tweet_count = Counter()
            date_user_count = defaultdict(Counter)
            
            for future in as_completed(futures):
                chunk_tweet_count, chunk_user_count = future.result()
                date_tweet_count.update(chunk_tweet_count)
                for date, user_count in chunk_user_count.items():
                    date_user_count[date].update(user_count)
    
    top_10_dates = heapq.nlargest(10, date_tweet_count.items(), key=lambda x: x[1])
    
    result = [
        (date, max(date_user_count[date], key=date_user_count[date].get))
        for date, _ in top_10_dates
    ]
    return result

if __name__ == "__main__":
    import time
    
    file_path = JSON_FILENAME
    JsonMaker()

    start_time = time.time()
    top_senders = q1_time(file_path)
    end_time = time.time()
    print("Los 10 remitentes más activos:")
    for date, most_active_user in top_senders:
        print(f"Fecha: {date}, Usuario más activo: {most_active_user}")

    print(f"\nTiempo de ejecución: {end_time - start_time:.4f} segundos")