# import pandas as pd
#
# class ParquetReader:
#     def process(self,file_path):
#         try:
#             df = pd.read_parquet(file_path, engine='pyarrow')
#             return df
#         except Exception as e:
#             print(f"file reading error: {e}")
#             return None
#

import os
import pandas as pd

class ParquetReader:
    def process(self, file_path, include_subfolders=False):
        try:
            if include_subfolders and os.path.isdir(file_path):
                parquet_files = []
                for root, dirs, files in os.walk(file_path):
                    for file in files:
                        if file.endswith('.parquet'):
                            parquet_files.append(os.path.join(root, file))
                if not parquet_files:
                    print(f"No parquet files found in {file_path}")
                    return None
                dfs = [pd.read_parquet(f, engine='pyarrow') for f in parquet_files]
                return pd.concat(dfs, ignore_index=True)
            else:
                return pd.read_parquet(file_path, engine='pyarrow')
        except Exception as e:
            print(f"file reading error: {e}")
            return None


