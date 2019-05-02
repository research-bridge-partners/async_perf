import aiobotocore
import asyncio
import boto3
from functools import wraps
from queue import Empty, Queue
import threading
import time
from typing import List


def timing_val(func):
    @wraps(func)
    def wrapper(*arg, **kw):
        """Source: http://www.daniweb.com/code/snippet368.html"""
        t1 = time.time()
        res = func(*arg, **kw)
        t2 = time.time()
        return (t2 - t1), res, func.__name__
    return wrapper


def async_timing_val(func):
    @wraps(func)
    async def wrapper(*arg, **kw):
        """Source: http://www.daniweb.com/code/snippet368.html"""
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
    threads = []
    for i in range(num_threads):
        if not key_queue.empty():
            threads.append(threading.Thread(target=retrieve_key_worker, args=(key_queue, results)))
            threads[-1].start()
        else:
            print(f'Only {i} threads were needed')
            break
    for thread in threads:
        thread.join()
    assert len(results) == len(keys)
    return results


@timing_val
def retrieve_in_thread_buggy(keys: List[str], num_threads=100):
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


if __name__ == '__main__':
    bucket = 'public-async-testing'
    s3_keys = [f'samples/{i}.txt' for i in range(20000)]

    loop = asyncio.get_event_loop()
    for num_objects in [100, 300, 1000, 3000, 10000, 2000]:
        keys_to_load = s3_keys[:num_objects]
        async_result = loop.run_until_complete(retrieve_with_async(loop, keys_to_load))
        print(f'async retrieved {num_objects} objects in {async_result[0]}')
        for n_threads in [10, 30, 50, 100, 200]:
            thread_result = retrieve_in_thread(keys_to_load, n_threads)
            buggy_result = retrieve_in_thread_buggy(keys_to_load)
            print(f'threading retrieved {num_objects} objects in {thread_result[0]} seconds using {n_threads} threads')
            print(f'Buggy threading retrieved {num_objects} objects in {buggy_result[0]}')
        buggy_result = retrieve_in_thread_buggy(keys_to_load)
