import aiobotocore
import asyncio
import boto3
from functools import wraps
import json
from queue import Empty, Queue
import threading
import time
from typing import List


with open('500k_urls.json') as f:
    s3_keys = json.load(f)

bucket = 'rbp-dev'


def timing_val(func):
    @wraps(func)
    def wrapper(*arg, **kw):
        '''source: http://www.daniweb.com/code/snippet368.html'''
        t1 = time.time()
        res = func(*arg, **kw)
        t2 = time.time()
        return (t2 - t1), res, func.__name__
    return wrapper


def async_timing_val(func):
    @wraps(func)
    async def wrapper(*arg, **kw):
        '''source: http://www.daniweb.com/code/snippet368.html'''
        t1 = time.time()
        res = await func(*arg, **kw)
        t2 = time.time()
        return (t2 - t1), res, func.__name__
    return wrapper

@async_timing_val
async def retrieve_with_async(loop, keys: List[str]):
    
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client('s3') as s3_client:

        async def get_and_read(key):
            response = await s3_client.get_object(Bucket=bucket, Key=key)
            async with response['Body'] as stream:
                return await stream.read()

        return await asyncio.gather(*[get_and_read(key) for key in keys])


def retrieve_key_worker(key_queue: Queue, results):
    # We need one client per thread
    s3_client = boto3.session.Session().client('s3')
    while True:
        try:
            key = key_queue.get_nowait()
            
            results.append(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read())
        except Empty:
            break

@timing_val
def retrieve_in_thread(keys: List[str], num_threads=100):
        key_queue = Queue()
        results = []
        for key in keys:
            key_queue.put(key)
        # Create threads, start them, and then wait for them
        threads = [threading.Thread(target=retrieve_key_worker, args=(key_queue, results)) for _ in range(num_threads)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert len(results) == len(keys)
        return results


for num_objects in [100, 1000, 10000, 100000]:
    keys = s3_keys[:num_objects]
    loop = asyncio.get_event_loop()
    async_result = loop.run_until_complete(retrieve_with_async(loop, keys))
    thread_result = retrieve_in_thread(keys)
    print(f'async retrieved {num_objects} objects in {async_result[0]}')
    print(f'threading retrieved {num_objects} objects in {thread_result[0]}')

for num_objects in [100, 500, 1000, 5000, 10000]:
    for num_threads in [10, 20, 30, 50, 75, 100, 150]:
        keys = s3_keys[:num_objects]
        thread_result = retrieve_in_thread(keys, num_threads)
        print(f'threading retrieved {num_objects} objects in {thread_result[0]} seconds using {num_threads} threads')
