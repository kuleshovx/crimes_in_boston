
# Import pyspark.pandas
import pyspark.pandas as ps

# Create pandas DataFrame
technologies   = ({
    'Courses':["Spark","PySpark","Hadoop","Python","Pandas","Hadoop","Spark","Python","NA"],
    'Fee' :[22000,25000,23000,24000,26000,25000,25000,22000,1500],
    'Duration':['30days','50days','55days','40days','60days','35days','30days','50days','40days'],
    'Discount':[1000,2300,1000,1200,2500,None,1400,1600,0]
          })
df = ps.DataFrame(technologies)
print(df)

# Use groupby() to compute the sum
df2 = df.groupby(['Courses']).sum()
print(df2)
