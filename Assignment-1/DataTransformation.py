from pyspark.sql import functions as F

class DataTransformer:
    def transform(self, facebook_data, google_data):
        # Combine the Facebook and Google Ads data
        combined_data = facebook_data.unionByName(google_data)

        # Apply transformations
        combined_data = combined_data.fillna({'impressions': 0, 'clicks': 0, 'spend': 0})
        
        combined_data = combined_data.withColumn("source", 
                                                 F.when(F.col("ad_name").isNotNull(), "facebook")
                                                  .otherwise("google"))
        
        return combined_data