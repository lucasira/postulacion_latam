from memory_profiler import profile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import os
from itertools import islice
from typing import List, Tuple, Generator
import orjson
import emoji
from config import JSON_FILENAME
from JsonMaker import JsonMaker

EMOJI_SET = frozenset(emoji.EMOJI_DATA)
CHUNK_SIZE = 10000  

def extract_emojis(text: str) -> Generator[str, None, None]:
    return (char for char in text if char in EMOJI_SET)

def process_chunk(chunk: List[str]) -> Counter:
    emoji_counter = Counter()
    for line in chunk:
        if line.strip():
            try:
                tweet = orjson.loads(line)
                emoji_counter.update(extract_emojis(tweet['content']))
            except (orjson.JSONDecodeError, KeyError):
                continue
    return emoji_counter

def file_chunk_generator(file, chunk_size: int) -> Generator[List[str], None, None]:
    while True:
        chunk = list(islice(file, chunk_size))
        if not chunk:
            break
        yield chunk

# @profile descomentar para ver detalles
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    total_emoji_counter = Counter()
    num_threads = min(os.cpu_count() or 1, 4)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        with open(file_path, 'r') as file:
            for chunk_emoji_counter in executor.map(process_chunk, file_chunk_generator(file, CHUNK_SIZE)):
                total_emoji_counter.update(chunk_emoji_counter)

    return total_emoji_counter.most_common(10)

if __name__ == "__main__":
    import time
    
    file_path = JSON_FILENAME
    JsonMaker()

    start_time = time.time()
    top_emojis = q2_memory(file_path)
    end_time = time.time()
    
    print("Los 10 emojis más utilizados:")
    for emoji_char, count in top_emojis:
        print(f"{emoji_char} : {count}")

    print(f"\nTiempo de ejecución: {end_time - start_time:.4f} segundos")