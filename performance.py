import asyncio
import botocore
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty, Queue
import threading
import aiobotocore
from aiobotocore.config import AioConfig
import boto3
import pandas as pd
from time_util import async_timing_val, timing_val

BUCKET = 'public-async-testing'


@async_timing_val
async def retrieve_with_async(loop, keys, concurrency):

    session = aiobotocore.get_session(loop=loop)
    config = AioConfig(max_pool_connections=concurrency)
    async with session.create_client('s3', config=config) as s3_client:

        async def get_and_read(key):
            response = await s3_client.get_object(Bucket=BUCKET, Key=key)
            async with response['Body'] as stream:
                return await stream.read()

        return await asyncio.gather(*[get_and_read(key) for key in keys])


def retrieve_key_worker(key_queue, results):
    # We need one client per thread
    s3_client = None
    while True:
        try:
            key = key_queue.get_nowait()
            s3_client = boto3.session.Session().client('s3') if s3_client is None else s3_client
            results.append(s3_client.get_object(Bucket=BUCKET, Key=key)['Body'].read())
        except Empty:
            break


@timing_val
def retrieve_with_thread_executor(keys, num_threads=10):
    results = []
    futures = []
    key_queue = Queue()
    for key in keys:
        key_queue.put(key)
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for _ in range(num_threads):
            futures.append(executor.submit(retrieve_key_worker, key_queue, results))
    for f in as_completed(futures):
        if f.exception() is not None:
            print(f.exception())
    assert len(results) == len(keys), f'Only {len(results)} results returned.'
    return results


@timing_val
def retrieve_with_threads(keys, num_threads=100):
    """ This is equivalent to the ThreadPoolExecutor example above but doesn't keep track of the threads as closely.
    Since it gives exactly the same performance, we excluded it from the comparisons below."""
    key_queue = Queue()
    results = []
    for key in keys:
        key_queue.put(key)
    # Create threads, start them, and then wait for them
    threads = []
    for i in range(num_threads):
        threads.append(threading.Thread(target=retrieve_key_worker, args=(key_queue, results)))
        threads[-1].start()
    for thread in threads:
        thread.join()
    assert len(results) == len(keys)
    return results


def test_num_objects():
    s3_keys = [f'samples/{i}.txt' for i in range(30000)]
    test_results = []
    loop = asyncio.get_event_loop()
    for num_objects in [100, 300, 1000, 3000, 10000, 30000]:
        keys_to_load = s3_keys[:num_objects]
        for concurrency in [10, 30, 75]:
            async_result = loop.run_until_complete(retrieve_with_async(loop, keys_to_load, concurrency=concurrency))
            test_results.append(('Async', num_objects, concurrency, async_result[0]))
            print(test_results[-1])
            thread_result = retrieve_with_thread_executor(keys_to_load, concurrency)
            test_results.append(('Threading', num_objects, concurrency, thread_result[0]))
            print(test_results[-1])
    result_df = pd.DataFrame(test_results, columns=['method', 'num_objects', 'num_threads', 'time'])
    result_df.to_csv('num_object_results.csv', index=False)


def test_concurrency():
    test_results = []
    keys_to_load = [f'samples/{i}.txt' for i in range(30000)]
    loop = asyncio.get_event_loop()
    for concurrency in [10, 20, 30, 50, 75, 100, 150, 200]:
        async_result = loop.run_until_complete(retrieve_with_async(loop, keys_to_load, concurrency=concurrency))
        test_results.append(('Async', concurrency, async_result[0]))
        print(test_results[-1])
        thread_result = retrieve_with_thread_executor(keys_to_load, concurrency)
        test_results.append(('Threading', concurrency, thread_result[0]))
        print(test_results[-1])
    result_df = pd.DataFrame(test_results, columns=['method', 'num_threads', 'time'])
    result_df.to_csv('num_threads_results.csv', index=False)


def load_100k_objects():
    async_concurrency = 200
    num_threads = 50
    keys_to_load = [f'samples/{i}.txt' for i in range(100000)]
    thread_result = retrieve_with_thread_executor(keys_to_load, num_threads)
    print(f'Successfully loaded 100000 objects in {thread_result[0]} with {num_threads} threads')
    try:
        loop = asyncio.get_event_loop()
        async_result = loop.run_until_complete(retrieve_with_async(loop, keys_to_load, concurrency=async_concurrency))
        print(f'Successfully loaded 100000 objects in {async_result[0]} with concurrency={async_concurrency}')
    except botocore.exceptions.ClientError as e:
        print(e)


if __name__ == '__main__':
    test_num_objects()
    test_concurrency()
    load_100k_objects()
