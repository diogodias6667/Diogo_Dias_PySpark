from pyspark.sql import SparkSession

#Create Spark Session

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Diogo") \
    .getOrCreate()


#Data:

dataset_1 = "C:/Users/diogo/Documents/GitHub/Diogo_Dias_PySpark/datasets/dataset_one.csv"
dataset_2 = "C:/Users/diogo/Documents/GitHub/Diogo_Dias_PySpark/datasets/dataset_two.csv"
remove_countrys = ["United Kingdom", "Netherlands"]


#Read CSV Files

df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv(dataset_1)

df2 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv(dataset_2)

#Data should be joined using the **id** field

df = df.join(df2, ['id'])


#Remove personal identifiable information from the first dataset (Excluding emails)
#Only use clients from the **United Kingdom** or the **Netherlands**
#Remove credit card number from the second dataset

df = df.filter(df.country.isin(remove_countrys)).drop(df.email)


#Remove credit card number from the second dataset

df = df.drop(df.cc_n)



#Rename the columns for the easier readability to the business users

df = df.withColumnRenamed("id", "client_identifier")\
    .withColumnRenamed("btc_a", "bitcoin_address")\
    .withColumnRenamed("cc_t", "credit_card_type")


#Save the output in a **client_data** directory in the root directory of the project

df.write.option("header",True).csv("C:/Users/diogo/Documents/GitHub/Diogo_Dias_PySpark/client_data")
