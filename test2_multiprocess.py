import multiprocessing as mp
from faker import Faker
import pandas as pd
import random
import pyarrow as pa
import pyarrow.parquet as pq
import os
from math import ceil

# CONFIG
NUM_ROWS = 1_000_000
GEN_CHUNK_ROWS = 50_000    # rows per generated chunk (tune by memory)
QUEUE_MAX_CHUNKS = 4      # how many chunks each queue can hold before producers block
OUTPUT_CSV = "employees1.csv"
OUTPUT_PARQUET = "employees1.parquet"

def generate_worker(start_id, end_id, q_csv, q_parquet, chunk_rows):
    fake = Faker()
    empid = start_id
    while empid < end_id:
        chunk_end = min(empid + chunk_rows, end_id)
        records = []
        for eid in range(empid, chunk_end):
            records.append({
                "empid": eid,
                "name": fake.name(),
                "salary": round(random.uniform(30_000, 150_000), 2),
                # Keep as python date (will be converted later)
                "salary_date": fake.date_between(start_date='-1y', end_date='today')
            })
        # Put same chunk into both queues (blocks if queue full)
        q_csv.put(records)
        q_parquet.put(records)
        empid = chunk_end
    # finished this generator
    return

def csv_writer(q_csv, filename, num_generators):
    """Consume chunks and write to CSV streaming. Exit after receiving sentinel counts."""
    print("CSV writer started")
    header_written = False
    finished_count = 0
    with open(filename, "w", newline="", encoding="utf-8") as f:
        while True:
            chunk = q_csv.get()
            if chunk is None:
                finished_count += 1
                if finished_count >= 1:  # we will put a single sentinel once all producers done
                    break
                else:
                    continue
            df = pd.DataFrame(chunk)
            df.to_csv(f, index=False, header=not header_written)
            header_written = True
    print("CSV writer finished")

def parquet_writer(q_parquet, filename, num_generators):
    """Stream-write Parquet using pyarrow. Close writer at end."""
    print("Parquet writer started")
    writer = None
    finished = False
    while True:
        chunk = q_parquet.get()
        if chunk is None:
            # we expect a single sentinel after producers finish
            break

        df = pd.DataFrame(chunk)
        # convert salary_date to datetime64 (keeps real datetime in parquet)
        df["salary_date"] = pd.to_datetime(df["salary_date"])
        # convert pandas df to arrow table
        table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:
            # create ParquetWriter with schema from first chunk
            writer = pq.ParquetWriter(filename, table.schema, compression="snappy")
        writer.write_table(table)

    if writer:
        writer.close()
    print("Parquet writer finished")

# if __name__ == "__main__":
#     # leave 1 core free (tweak if you want)
#     cpu_cores = max(1, mp.cpu_count() - 1)
#     print(f"Detected CPU cores: {mp.cpu_count()}, using {cpu_cores} generators")

#     # compute how many rows per generator
#     base = NUM_ROWS // cpu_cores
#     extras = NUM_ROWS % cpu_cores

#     # queues with small maxsize to provide backpressure
#     q_csv = mp.Queue(maxsize=QUEUE_MAX_CHUNKS)
#     q_parquet = mp.Queue(maxsize=QUEUE_MAX_CHUNKS)

#     # start writers (they will run until sentinel)
#     p_csv = mp.Process(target=csv_writer, args=(q_csv, OUTPUT_CSV, cpu_cores), daemon=False)
#     p_parquet = mp.Process(target=parquet_writer, args=(q_parquet, OUTPUT_PARQUET, cpu_cores), daemon=False)
#     p_csv.start()
#     p_parquet.start()

#     # spawn generators
#     producers = []
#     next_id = 1
#     for i in range(cpu_cores):
#         # allocate the extra rows to the first `extras` workers
#         this_count = base + (1 if i < extras else 0)
#         start_id = next_id
#         end_id = start_id + this_count
#         next_id = end_id
#         p = mp.Process(target=generate_worker, args=(start_id, end_id, q_csv, q_parquet, GEN_CHUNK_ROWS))
#         p.start()
#         producers.append(p)

#     # wait for generators to finish
#     for p in producers:
#         p.join()

#     # signal writers that production finished (single sentinel each)
#     q_csv.put(None)
#     q_parquet.put(None)

#     # wait for writers
#     p_csv.join()
#     p_parquet.join()

#     # Print sizes
#     print("Done. File sizes:")
#     for fname in (OUTPUT_CSV, OUTPUT_PARQUET):
#         if os.path.exists(fname):
#             print(f"  {fname}: {os.path.getsize(fname)/1024/1024:.2f} MB")
#         else:
#             print(f"  {fname}: not found")

import time  # <-- already needed

if __name__ == "__main__":
    start_time = time.time()  # record start time

    # leave 1 core free (tweak if you want)
    cpu_cores = max(1, mp.cpu_count() - 1)
    print(f"Detected CPU cores: {mp.cpu_count()}, using {cpu_cores} generators")

    # compute how many rows per generator
    base = NUM_ROWS // cpu_cores
    extras = NUM_ROWS % cpu_cores

    # queues with small maxsize to provide backpressure
    q_csv = mp.Queue(maxsize=QUEUE_MAX_CHUNKS)
    q_parquet = mp.Queue(maxsize=QUEUE_MAX_CHUNKS)

    # start writers (they will run until sentinel)
    p_csv = mp.Process(target=csv_writer, args=(q_csv, OUTPUT_CSV, cpu_cores), daemon=False)
    p_parquet = mp.Process(target=parquet_writer, args=(q_parquet, OUTPUT_PARQUET, cpu_cores), daemon=False)
    p_csv.start()
    p_parquet.start()

    # spawn generators
    producers = []
    next_id = 1
    for i in range(cpu_cores):
        this_count = base + (1 if i < extras else 0)
        start_id = next_id
        end_id = start_id + this_count
        next_id = end_id
        p = mp.Process(target=generate_worker, args=(start_id, end_id, q_csv, q_parquet, GEN_CHUNK_ROWS))
        p.start()
        producers.append(p)

    # wait for generators to finish
    for p in producers:
        p.join()

    # signal writers that production finished (single sentinel each)
    q_csv.put(None)
    q_parquet.put(None)

    # wait for writers
    p_csv.join()
    p_parquet.join()

    end_time = time.time()  # record end time
    elapsed = end_time - start_time
    print(f"\n✅ All tasks completed successfully in {elapsed:.2f} seconds.")

    # Print sizes
    print("Done. File sizes:")
    for fname in (OUTPUT_CSV, OUTPUT_PARQUET):
        if os.path.exists(fname):
            print(f"  {fname}: {os.path.getsize(fname)/1024/1024:.2f} MB")
        else:
            print(f"  {fname}: not found")
