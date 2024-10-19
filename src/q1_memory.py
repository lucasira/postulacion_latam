from collections import Counter, defaultdict
import heapq
from concurrent.futures import ThreadPoolExecutor
import os
from itertools import islice
from typing import List, Tuple
from datetime import date as date_class
import orjson
from datetime import datetime
from config import JSON_FILENAME
from memory_profiler import profile
from JsonMaker import JsonMaker

# @profile descomentar para ver detalles
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    chunk_size = 1000
    date_tweet_count = Counter()
    user_tweet_count = defaultdict(lambda: defaultdict(int))
    
    num_threads = min(os.cpu_count() or 1, 4)
    
    def process_chunk(chunk):
        chunk_date_tweet_count = Counter()
        chunk_user_tweet_count = defaultdict(lambda: defaultdict(int))
        for line in chunk:
            tweet = orjson.loads(line)
            date_str = tweet['date'][:10]
            tweet_date = date_class(int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10]))
            username = tweet['user']['username']
            
            chunk_date_tweet_count[tweet_date] += 1
            chunk_user_tweet_count[tweet_date][username] += 1
        
        return chunk_date_tweet_count, chunk_user_tweet_count
    
    def file_chunk_generator(file, chunk_size):
        while True:
            chunk = list(islice(file, chunk_size))
            if not chunk:
                break
            yield chunk
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        with open(file_path, 'r') as file:
            for chunk_date_tweet_count, chunk_user_tweet_count in executor.map(process_chunk, file_chunk_generator(file, chunk_size)):
                date_tweet_count.update(chunk_date_tweet_count)
                for date, user_counts in chunk_user_tweet_count.items():
                    for user, count in user_counts.items():
                        user_tweet_count[date][user] += count
    
    top_10_dates = heapq.nlargest(10, date_tweet_count.items(), key=lambda x: x[1])
    
    result = []
    for tweet_date, tweet_count in top_10_dates:
        top_user = max(user_tweet_count[tweet_date].items(), key=lambda x: x[1])[0]
        result.append((tweet_date, top_user))
    
    return result

if __name__ == "__main__":
    import time
    
    file_path = JSON_FILENAME
    JsonMaker()
    start_time = time.time()
    top_senders = q1_memory(file_path)
    end_time = time.time()
    
    print("Los 10 remitentes más activos:")
    for date, most_active_user in top_senders:
        print(f"Fecha: {date}, Usuario más activo: {most_active_user}")

    print(f"\nTiempo de ejecución: {end_time - start_time:.4f} segundos")