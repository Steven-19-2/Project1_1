import multiprocessing as mp
from faker import Faker
import pandas as pd
import random
import pyarrow as pa
import pyarrow.parquet as pq
from math import ceil
import datetime
import time

NUM_ROWS = 1_000_000
GEN_CHUNK_ROWS = 50_000    # rows per generated chunk 
QUEUE_MAX_CHUNKS = 4      # how many chunks each queue can hold before producers block
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
OUTPUT_CSV = f"employees_{timestamp}.csv"
OUTPUT_PARQUET = f"employees_{timestamp}.parquet"

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
                "salary_date": fake.date_between(start_date='-1y', end_date='today')
            })
        
        q_csv.put(records)
        q_parquet.put(records)
        empid = chunk_end
    return

def csv_writer(q_csv, filename, num_generators):
    print("CSV writer started")
    header_written = False
    finished_count = 0
    with open(filename, "w", newline="", encoding="utf-8") as f:
        while True:
            chunk = q_csv.get()
            if chunk is None:
                finished_count += 1
                if finished_count >= num_generators: 
                    break
                else:
                    continue
            df = pd.DataFrame(chunk)
            df.to_csv(f, index=False, header=not header_written)
            header_written = True
    print("CSV writer finished")

def parquet_writer(q_parquet, filename, num_generators):
    print("Parquet writer started")
    writer = None
    finished_count = 0
    while True:
        chunk = q_parquet.get()
        if chunk is None:
            finished_count += 1
            if finished_count >= num_generators:
                break
            else:
                continue

        df = pd.DataFrame(chunk)
        df["salary_date"] = pd.to_datetime(df["salary_date"])
        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(filename, table.schema, compression="snappy")
        writer.write_table(table)
    if writer:
        writer.close()
    print("Parquet writer finished")



def main():
    start_time = time.time()  # start time

    cpu_cores = mp.cpu_count() - 1
    print(f"Detected CPU cores: {mp.cpu_count()}, using {cpu_cores} generators")

    base = NUM_ROWS // cpu_cores  #90909.0909 ~ 90909
    extras = NUM_ROWS % cpu_cores #1

    q_csv = mp.Queue(maxsize=QUEUE_MAX_CHUNKS) #maxsize prevents memory from growing too large.
    q_parquet = mp.Queue(maxsize=QUEUE_MAX_CHUNKS)

    """start writers"""
    p_csv = mp.Process(target=csv_writer, args=(q_csv, OUTPUT_CSV, cpu_cores), daemon=False)
    p_parquet = mp.Process(target=parquet_writer, args=(q_parquet, OUTPUT_PARQUET, cpu_cores), daemon=False)
    p_csv.start()
    p_parquet.start()

    """making the range"""
    producers = []
    next_id = 1
    for i in range(cpu_cores):
        this_count = base + (1 if i < extras else 0) #90909 + 1 #90909 + 0 ....
        start_id = next_id #1 # 90910
        end_id = start_id + this_count #1 + 90909 # 90910 + 90909
        next_id = end_id #90910 # 181819
        p = mp.Process(target=generate_worker, args=(start_id, end_id, q_csv, q_parquet, GEN_CHUNK_ROWS))
        p.start()
        producers.append(p)

    """ wait for generators to finish"""
    for p in producers:
        p.join()

    """tell writers that production finished """
    for _ in range(cpu_cores):
        q_csv.put(None)
        q_parquet.put(None)

    """wait for writers"""
    p_csv.join()
    p_parquet.join()

    end_time = time.time()  # record end time
    elapsed = end_time - start_time
    print(f"\nAll tasks completed successfully in {elapsed:.2f} seconds.")
    return OUTPUT_PARQUET


if __name__ == "__main__":
    main()
