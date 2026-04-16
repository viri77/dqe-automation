
import psycopg2
import pandas as pd


class PostgresConnectorContextManager:
    def __init__(self, db_host: str, db_name: str,user: str, password: str,port: int):
        self.db_host = db_host
        self.db_name = db_name
        self.user = user
        self.password = password
        self.port = port



    def __enter__(self):
        self.connection = psycopg2.connect(
            host=self.db_host,
            port=self.port,
            dbname=self.db_name,
            user=self.user,
            password=self.password
        )

        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.connection:
            self.connection.close()


    def get_data_sql(self, sql):
        cursor = self.connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        return df




