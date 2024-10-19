import orjson
from typing import List, Tuple
from datetime import datetime
from collections import Counter, defaultdict
import heapq

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    date_tweet_count = Counter()
    date_user_count = defaultdict(Counter)
    
    with open(file_path, 'r') as file:
        for line in file:
            try:
                tweet = orjson.loads(line)
                date_str = tweet['date'][:10]
                date = datetime(int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])).date()
                username = tweet['user']['username']
                
                date_tweet_count[date] += 1
                date_user_count[date][username] += 1
            except (KeyError, ValueError, orjson.JSONDecodeError) as e:
                print(f"Error processing line: {e}")
                continue
    
    top_10_heap = heapq.nlargest(10, date_tweet_count.items(), key=lambda x: x[1])
    
    result = []
    for date, _ in top_10_heap:
        top_user = max(date_user_count[date].items(), key=lambda x: x[1])[0]
        result.append((date, top_user))
    
    return result

if __name__ == "__main__":
    import time
    
    file_path = "extracted_files/farmers-protest-tweets-2021-2-4.json"
    
    start_time = time.time()
    try:
        top_dates_users = q1_memory(file_path)
        end_time = time.time()
        
        print("Top 10 dates with the most tweets and their top users:")
        for date, user in top_dates_users:
            print(f"Date: {date}, Top User: {user}")
        
        print(f"\nExecution time: {end_time - start_time:.4f} seconds")
    except Exception as e:
        print(f"An error occurred: {e}")