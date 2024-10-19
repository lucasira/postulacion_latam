import orjson  # Use orjson for faster JSON parsing
from typing import List, Tuple
from datetime import datetime
from collections import Counter, defaultdict
import heapq

def q2_time(file_path: str) -> Tuple[List[Tuple[datetime.date, str]], int]:
    date_tweet_count = Counter()
    date_user_count = defaultdict(Counter)
    total_tweets = 0
    
    with open(file_path, 'r') as file:
        for line in file:
            tweet = orjson.loads(line)  # Use orjson for faster parsing
            date_str = tweet['date'][:10]
            date = datetime(int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])).date()  # Manual date parsing
            username = tweet['user']['username']
            
            date_tweet_count[date] += 1
            date_user_count[date][username] += 1
            total_tweets += 1
    
    # Use a heap to keep track of the top 10 dates
    top_10_heap = heapq.nlargest(10, date_tweet_count.items(), key=lambda x: x[1])
    
    result = []
    for date, _ in top_10_heap:
        top_user = max(date_user_count[date].items(), key=lambda x: x[1])[0]
        result.append((date, top_user))
    
    return result, total_tweets

# Example usage
if __name__ == "__main__":
    import time
    
    file_path = "extracted_files/farmers-protest-tweets-2021-2-4.json"  # Replace with the actual file path
    
    start_time = time.time()
    top_dates_users, total_tweets = q2_time(file_path)
    end_time = time.time()
    
    print(f"Total number of tweets processed: {total_tweets}")
    print("\nTop 10 dates with the most tweets and their top users:")
    for date, user in top_dates_users:
        print(f"Date: {date}, Top User: {user}")
    
    print(f"\nExecution time: {end_time - start_time:.4f} seconds")