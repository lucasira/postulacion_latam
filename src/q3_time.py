import orjson
from typing import List, Tuple
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import os
import mmap
import re
from config import JSON_FILENAME
from JsonMaker import JsonMaker

def extract_mentions(text: str) -> List[str]:
    return re.findall(r'@(\w+)', text)

def process_chunk(chunk: bytes) -> Counter:
    mention_counter = Counter()
    for line in chunk.splitlines():
        if line.strip():
            try:
                tweet = orjson.loads(line)
                mentions = extract_mentions(tweet['content'])
                mention_counter.update(mentions)
            except orjson.JSONDecodeError:
                continue
    return mention_counter

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    mention_counter = Counter()
    
    with open(file_path, 'rb') as file:
        file_size = os.fstat(file.fileno()).st_size
        num_processes = os.cpu_count() or 1
        chunk_size = max(1, file_size // num_processes)
        
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            chunks = [mm[i:i+chunk_size] for i in range(0, file_size, chunk_size)]
            
            for i in range(len(chunks) - 1):
                newline_pos = chunks[i].rfind(b'\n')
                if newline_pos != -1:
                    chunks[i + 1] = chunks[i][newline_pos + 1:] + chunks[i + 1]
                    chunks[i] = chunks[i][:newline_pos + 1]
            
            with ProcessPoolExecutor(max_workers=num_processes) as executor:
                for chunk_mention_count in executor.map(process_chunk, chunks):
                    mention_counter.update(chunk_mention_count)
    
    return mention_counter.most_common(10)

if __name__ == "__main__":
    import time
    
    file_path = JSON_FILENAME
    JsonMaker()

    start_time = time.time()
    top_influential_users = q3_time(file_path)
    end_time = time.time()
    
    print("Los 10 usuarios históricos más influyentes:")
    for username, count in top_influential_users:
        print(f"@{username} : {count} menciones")

    print(f"\nTiempo de ejecución: {end_time - start_time:.4f} segundos")