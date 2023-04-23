from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert
import pandas as pd
import gc

class ChangeDb():
    def __init__(self, driver: str, username: str, password: str, host: str, schema: str, port = ""):
        self.engine = create_engine(rf'{driver}://{username}:{password}@{host}/{schema}')
    
    def _insert_on_duplicate(self, table, conn, keys, data_iter):
        insert_stmt = insert(table.table).values(list(data_iter))
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
        conn.execute(on_duplicate_key_stmt)

    def update_or_insert(self, path: str, table_name: str, type: str) -> None:
        if type == 'xlsx':
            updated_data = pd.read_excel(path)
        elif type == 'csv':
            updated_data = pd.read_csv(path)
        else:
            raise Exception('Invalid type. Please, use "xlsx" or "csv"')

        with self.engine.connect() as conn:
            conn.execute(text("set foreign_key_checks=0;"))

        j=0
        for i in range(0, len(updated_data), 10000):
            j+=10000
            sent_data = updated_data[i:j]

            sent_data.to_sql(name=table_name, con=self.engine, if_exists='append', index=False, method = self._insert_on_duplicate)
            del sent_data; gc.collect()