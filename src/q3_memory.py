from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import os
from itertools import islice
from typing import List, Tuple
import re
import orjson
from memory_profiler import profile
from config import JSON_FILENAME
from JsonMaker import JsonMaker

# @profile descomentar para ver detalles
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    chunk_size = 1000
    mention_count = Counter()
    
    num_threads = min(os.cpu_count() or 1, 4)
    
    def process_chunk(chunk):
        chunk_mention_count = Counter()
        for line in chunk:
            tweet = orjson.loads(line)
            content = tweet.get('content', '')
            mentions = re.findall(r'@(\w+)', content)
            chunk_mention_count.update(mentions)
        return chunk_mention_count
    
    def file_chunk_generator(file, chunk_size):
        while True:
            chunk = list(islice(file, chunk_size))
            if not chunk:
                break
            yield chunk
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        with open(file_path, 'r') as file:
            for chunk_mention_count in executor.map(process_chunk, file_chunk_generator(file, chunk_size)):
                mention_count.update(chunk_mention_count)
    
    top_10_users = mention_count.most_common(10)
    
    return top_10_users

if __name__ == "__main__":
    import time
    
    file_path = JSON_FILENAME
    JsonMaker()
    start_time = time.time()
    top_influential_users = q3_memory(file_path)
    end_time = time.time()
    
    print("Los 10 usuarios históricos más influyentes:")
    for username, count in top_influential_users:
        print(f"@{username} : {count} menciones")

    print(f"\nTiempo de ejecución: {end_time - start_time:.4f} segundos")