import requests
import yaml
from pyspark.sql import SparkSession

class ConfigLoader:
    """Load configuration from a YAML file."""
    @staticmethod
    def load_config(config_path):
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
        
class BaseExtractor:
    def extract(self):
        pass

class FacebookExtractor(BaseExtractor):
    def __init__(self, access_token, ad_account_id, spark_session):
        self.access_token = access_token
        self.ad_account_id = ad_account_id
        self.spark_session = spark_session

    def extract(self):
        url = f"https://graph.facebook.com/v12.0/{self.ad_account_id}/insights"
        params = {
            "access_token": self.access_token,
            "date_preset": "last_30_days",
            "fields": "impressions,clicks,spend,ad_name"
        }
        response = requests.get(url, params=params)
        data = response.json().get('data', [])
        return self.spark_session.createDataFrame(data)

class GoogleExtractor(BaseExtractor):
    def __init__(self, credentials, client_id, customer_id, spark_session):
        self.credentials = credentials
        self.client_id = client_id
        self.customer_id = customer_id
        self.spark_session = spark_session

    def extract(self):
        pass
