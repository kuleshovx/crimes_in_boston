# Import pyspark.pandas
import pyspark.pandas as ps
import findspark


findspark.init()

crime_df = ps.read_csv("data/crime.csv")

print(crime_df)