import asyncio
import threading
import time
import concurrent.futures
import threading
import queue

import requests
import json
from pathlib import Path
import numpy as np
from decouple import config
import multiprocessing
import queue
from stockx import get_all_subcategories,get_all_thirdlevel
import stockx
from tools import formatted_products, post_products

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

SERVER_NUMBER = int(config('SERVER_NUMBER'))
NUM_PROCESSES = int(config('NUM_PROCESSES'))
NUM_PROCESSES=5

def run_async_scrape_slugs(category):
    loop = asyncio.get_event_loop()
    slugs = loop.run_until_complete(stockx.scrape_slugs(category))
    return slugs


def run_async_scrape_product(url, result_queue):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # loop = asyncio.get_event_loop()
    time.sleep(0.4)
    product = loop.run_until_complete(stockx.scrape_product(url))
    result_queue.put(product)
    # return product






async def run():
    stockx.BASE_CONFIG["cache"] = True

    categories = stockx.get_all_categories()
    limited_categories = ['shoes' 'handbags','men','kids','home'] #'''women's clothing''']
    subcategories = [get_all_subcategories(categories[i]) for i in limited_categories]
    subcategories = {key: value for dictionary in subcategories for key, value in dictionary.items()}
    third_level = [get_all_thirdlevel(value) for value in subcategories.values()]
    final_categories = [value for dictionary in third_level for value in dictionary.values() if value!='']
    split_categories = np.array_split(final_categories, 5)
    categories_by_server_number = split_categories[SERVER_NUMBER - 1]
    final_categories = ['https://www.neimanmarcus.com/en-kz/c' + item for item in final_categories]
    formatted_categories_by_process_number = np.array_split(final_categories, len(final_categories) / 5)
    for c in formatted_categories_by_process_number:
        pool = multiprocessing.Pool(processes=NUM_PROCESSES)
        slugs = pool.map(run_async_scrape_slugs, c.tolist())
        slugs = [item for sublist in slugs for item in sublist]
        pool.close()
        pool.join()
        with open('results/slugs_new.json', 'r') as f1:
            s = json.load(f1)
        new_slugs = s + slugs
        with open('results/slugs_new.json', 'w') as f2:
            json.dump(new_slugs, f2)
        print("All processes have completed successfully")

    with open('results/slugs_new.json', 'r') as f1:
        slugs = np.array(json.load(f1))
    slugs = [dict(s) for s in set(frozenset(d.items()) for d in slugs)]
    slugs = [
        "https://www.neimanmarcus.com/en-kz" + slug_by_server_number.get('slug')
        for slug_by_server_number in slugs
    ]
    formatted_slugs = np.array_split(
        slugs, len(slugs) / NUM_PROCESSES
    )
    # for slug_parts in formatted_slugs:
    #     pool = multiprocessing.Pool(processes=NUM_PROCESSES)
    #     products = pool.map(run_async_scrape_product, slug_parts.tolist())
    #     pool.close()
    #     pool.join()
    #     products_formatted = formatted_products(products)
    #     post_products(products_formatted)
    #     print("All processes have completed successfully")

    for slug_parts in formatted_slugs:
        result_queue = queue.Queue()
        threads = []
        for i in slug_parts.tolist():
            thread = threading.Thread(target=run_async_scrape_product, args=(i, result_queue))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        #products_formatted = formatted_products(results)
        post_products(results)
        print("All treads have completed successfully")


if __name__ == "__main__":
    asyncio.run(run())
