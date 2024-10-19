import orjson
from typing import List, Tuple
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import os
import mmap
import emoji
from config import JSON_FILENAME
from JsonMaker import JsonMaker

EMOJI_SET = frozenset(emoji.EMOJI_DATA)

def extract_emojis(text: str) -> List[str]:
    return [char for char in text if char in EMOJI_SET]

def process_chunk(chunk: bytes) -> Counter:
    emoji_counter = Counter()
    for line in chunk.splitlines():
        if line.strip():
            try:
                tweet = orjson.loads(line)
                emojis = extract_emojis(tweet['content'])
                emoji_counter.update(emojis)
            except (orjson.JSONDecodeError, KeyError):
                continue
    return emoji_counter

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    emoji_counter = Counter()
    num_processes = os.cpu_count()
    chunk_size = 10 * 1024 * 1024  # 10MB chunks

    with open(file_path, 'rb') as file:
        file_size = os.fstat(file.fileno()).st_size
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            with ProcessPoolExecutor(max_workers=num_processes) as executor:
                futures = []
                for i in range(0, file_size, chunk_size):
                    chunk = mm[i:i+chunk_size]
                    if i + chunk_size < file_size:
                        chunk = chunk[:chunk.rfind(b'\n')+1]
                    futures.append(executor.submit(process_chunk, chunk))
                
                for future in futures:
                    emoji_counter.update(future.result())

    return emoji_counter.most_common(10)

if __name__ == "__main__":
    import time
    
    file_path = JSON_FILENAME
    JsonMaker()

    start_time = time.time()
    top_emojis = q2_time(file_path)
    end_time = time.time()
    
    print("Los 10 emojis más utilizados:")
    for emoji_char, count in top_emojis:
        print(f"{emoji_char} : {count}")

    print(f"\nTiempo de ejecución: {end_time - start_time:.4f} segundos")