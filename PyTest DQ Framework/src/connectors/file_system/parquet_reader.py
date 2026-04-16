import pandas as pd

class ParquetReader:
    def process(self,file_path):
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            return df
        except Exception as e:
            print(f"file reading error: {e}")
            return None



