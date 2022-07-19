import pyarrow.parquet as pq


df = pq.read_table(source="test.parquet").to_pandas()
print(df)