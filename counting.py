import pyarrow.parquet as pq
table = pq.read_table(r"C:\Users\ZML-WIN-StevenD-01\Desktop\Project1_1\employees1.parquet")
print("Total rows in Parquet:", table.num_rows)
