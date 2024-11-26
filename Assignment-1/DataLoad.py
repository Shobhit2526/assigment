from pyspark.sql import DataFrame

class DataLoader:
    def __init__(self, db_url, db_properties):
        self.db_url = db_url
        self.db_properties = db_properties

    def load(self, data: DataFrame, table_name: str):
        data.write.jdbc(url=self.db_url, table=table_name, mode='append', properties=self.db_properties)