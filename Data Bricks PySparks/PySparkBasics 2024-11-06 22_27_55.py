# Databricks notebook source
#CreateRDDFromList
lst = [1,2,3,4,5,6,7,8]

# COMMAND ----------

print(lst)

# COMMAND ----------

type(lst)

# COMMAND ----------

sparksc = spark.sparkContext

# COMMAND ----------

type(sparksc)

# COMMAND ----------

rdd = sparksc.parallelize(lst)

# COMMAND ----------

type(rdd)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

lst2 = [2,4,6,8,1,5,9]
rdd2 = sc.parallelize(lst2)

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create RDD from Textfile**

# COMMAND ----------

rdd = sc.textFile("/FileStore/tables/001_Wordcount-1.txt")

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd_2 = rdd.flatMap(lambda x : x.split(" "))

# COMMAND ----------

rdd_2.collect()

# COMMAND ----------

rdd_3 = rdd_2.map(lambda a : (a,1))

# COMMAND ----------

rdd_3.collect()

# COMMAND ----------

rdd_4 = rdd_3.reduceByKey(lambda x,y : x + y)

# COMMAND ----------

rdd_4.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Sorting and Extractng From RDD**

# COMMAND ----------

rdd_5 = rdd_4.sortByKey()

# COMMAND ----------

rdd_5.collect()

# COMMAND ----------

rdd_6 = rdd_4.sortByKey(False)

# COMMAND ----------

rdd_6.collect()

# COMMAND ----------

rdd_7 = rdd_4.sortBy(lambda x : x[1])

# COMMAND ----------

rdd_7.collect()  #It show the value in accending order

# COMMAND ----------

rdd_8 = rdd_4.sortBy(lambda x : x[1], False)

# COMMAND ----------

rdd_8.collect()

# COMMAND ----------

r = rdd_4.first()

# COMMAND ----------

type(r)

# COMMAND ----------

r[0]

# COMMAND ----------

r[1]

# COMMAND ----------

y = rdd_8.take(4)

# COMMAND ----------

y

# COMMAND ----------

type(y)

# COMMAND ----------

rdd_7.take(4)

# COMMAND ----------

rddkeys = rdd_4.keys()

# COMMAND ----------

rddkeys.collect()

# COMMAND ----------

rddval = rdd_4.values()

# COMMAND ----------

rddval.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Filter in RDD**

# COMMAND ----------

rdd_4.filter(lambda x : x[1]>30).collect()

# COMMAND ----------

rdd_4.filter(lambda x : x[1] % 2 == 0).collect()

# COMMAND ----------

rdd_4.filter(lambda x : x[0].startswith("h")).collect()

# COMMAND ----------

rdd_4.filter(lambda x : x[0].endswith("y")).collect()

# COMMAND ----------

rdd_4.filter(lambda x : "e" in x[0]).collect()

# COMMAND ----------

rdd_4.filter(lambda x : x[0].find("e") != -1).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Save RDD as Textfile**

# COMMAND ----------

rdd_4.collect()

# COMMAND ----------

rdd_4.saveAsTextFile("/FileStore/tables/PysparkFles")

# COMMAND ----------

display(spark.read.text("/FileStore/tables/PysparkFles"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create DataFrame From RDD**

# COMMAND ----------

rdd = sc.parallelize([(1,'India'),(2,'Nepal'),(3,'Pak'),(4,'US')])

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd.count()

# COMMAND ----------

df = rdd.toDF()  #It return the dataframe 

# COMMAND ----------

df.collect()

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

myschema = ["id","Country"]

# COMMAND ----------

df_2 = rdd.toDF(myschema)

# COMMAND ----------

df_2.show()

# COMMAND ----------

df_2.printSchema()

# COMMAND ----------

df_3 = rdd.toDF("id integer, country string")

# COMMAND ----------

df_3.show()

# COMMAND ----------

df_3.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, DateType, TimestampType

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

mydfSchema = StructType([
                StructField("id",LongType(), True),
                StructField("Country",StringType(), True)
]
)

# COMMAND ----------

df_4 = rdd.toDF(mydfSchema)

# COMMAND ----------

df_4.show()

# COMMAND ----------

df_4.printSchema()

# COMMAND ----------

df_5 = spark.createDataFrame(rdd)

# COMMAND ----------

df_5.show()

# COMMAND ----------

df_5.printSchema()

# COMMAND ----------

df_6 = spark.createDataFrame(rdd,mydfSchema)
df_6.show()

# COMMAND ----------

df_6.printSchema()

# COMMAND ----------

df_7 = spark.createDataFrame(rdd, "id long, country string")
df_7.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_7.columns

# COMMAND ----------

len(df_7.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC **Read CSV File in DataFrame**

# COMMAND ----------

df_csv = spark.read.csv("/FileStore/tables/Data.csv")

# COMMAND ----------

df_csv.show()

# COMMAND ----------

df_csv.columns

# COMMAND ----------

display(df_csv)

# COMMAND ----------

df_csv2 = spark.read.csv("/FileStore/tables/Data.csv", header="True")
df_csv2.show()

# COMMAND ----------

df_csv2.printSchema()

# COMMAND ----------

df_csv2.dtypes

# COMMAND ----------

df_csv3 = spark.read.csv("/FileStore/tables/Data.csv", header="True", inferSchema="True")
df_csv3.show()

# COMMAND ----------

df_csv3.printSchema()

# COMMAND ----------

df_csv4 = spark.read.format("csv") \
        .option("path","/FileStore/tables/Data.csv") \
        .option("header","True") \
        .option("inferSchema","True").load()
df_csv4.show()

# COMMAND ----------

df_csv4 = (spark.read.format("csv") 
        .option("path","/FileStore/tables/Data.csv") 
        .option("header","True") 
        .option("inferSchema","True").load())
df_csv4.show()

# COMMAND ----------

df_csv4.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Write DataFrame**

# COMMAND ----------

df_csv.write.format("csv").save('/FileStore/tables/PySparkData.csv')

# COMMAND ----------

df_csv.write.format("parquet").save('/FileStore/tables/PySparkData2.csv')

# COMMAND ----------

df_csv.write.format("csv").option("path","/FileStore/tables/PySparkData3.csv").save()

# COMMAND ----------

df_csv.write.format("csv").option("path","/FileStore/tables/PySparkData3.csv").mode("append").save()

# COMMAND ----------

df_csv.write.format("csv").option("path","/FileStore/tables/PySparkData3.csv").mode("overwrite").save()

# COMMAND ----------

df_csv.write.format("csv").option("path","/FileStore/tables/PySparkData3.csv").mode("error").save()

# COMMAND ----------

df_csv.write.format("csv").option("path","/FileStore/tables/PySparkData3.csv").mode("ignore").save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Select Column from DataFrame**

# COMMAND ----------

df_csv = spark.read.csv("/FileStore/tables/Products.csv",header="True", inferSchema="True")
df_csv.show() 

# COMMAND ----------

display(df_csv)

# COMMAND ----------

df_csv1 = df_csv.select("OrderDate", "Country", "EnglishProductName")

# COMMAND ----------

df_csv1.show()

# COMMAND ----------

display(df_csv1)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_csv2 = df_csv1.select(col("OrderDate"),col("Country"),col("EnglishProductName")).show()

# COMMAND ----------

df_csv3 = df_csv.selectExpr("OrderDate","Country","EnglishProductName").show() 

# COMMAND ----------

display(df_csv3)

# COMMAND ----------

#We can write any expresson in selectExpr
df_csv4 = df_csv.selectExpr("OrderDate","Country","SalesAmount", "SalesAmount * 0.05 as Tax").show()

# COMMAND ----------

#We can use expression with select also but for that we need to import 
from pyspark.sql.functions import expr
df_scv5 = df_csv.select("OrderDate","Country","SalesAmount", expr("SalesAmount * 0.05 as Tax")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Add Columns to DataFrame**

# COMMAND ----------

df_csv_col = (spark.read.format("csv")
              .option("path","/FileStore/tables/Data.csv")
              .option("header", "True").load())
display(df_csv_col)

# COMMAND ----------

# We can add column with string using the following import  
from pyspark.sql.functions import lit, col

# COMMAND ----------

# WithColumn is used for add a new column and modifiey the existing column
df_col = df_csv_col.withColumn("Country",lit("India"))

# COMMAND ----------

display(df_col)

# COMMAND ----------

df_col2 = df_col.withColumn("Tax",col("Sales") * 0.05)

# COMMAND ----------

display(df_col2)

# COMMAND ----------

df_col.printSchema()

# COMMAND ----------

df_col2.printSchema()

# COMMAND ----------

#This is used for change the one data type into another
df_col_cast = df_col2.withColumn("ProductKey",col("ProductKey").cast("integer")).withColumn("Sales",col("Sales").cast("integer"))

# COMMAND ----------

df_col_cast.show()

# COMMAND ----------

df_col_cast.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Drops Column for DataFrame**

# COMMAND ----------

display(df_csv_col)

# COMMAND ----------

df_drop = df_csv_col.drop("Sales")

# COMMAND ----------

display(df_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC **Rename Column in DataFrame**

# COMMAND ----------

df_rename = df_csv_col.withColumnRenamed("ProductKey","ProductId")

# COMMAND ----------

display(df_rename)

# COMMAND ----------

df_rename.printSchema()

# COMMAND ----------

df_rename2 = df_csv_col.selectExpr("ProductKey as ProductId","ProducName","Sales")

# COMMAND ----------

display(df_rename2)

# COMMAND ----------

df_csv_col.columns

# COMMAND ----------

newcolumn = list(map(lambda x : x.upper(),df_csv_col.columns))

# COMMAND ----------

print(newcolumn)

# COMMAND ----------

df_csv_upper = df_csv_col.toDF(*newcolumn)

# COMMAND ----------

display(df_csv_upper)

# COMMAND ----------

# MAGIC %md
# MAGIC **Sort Data in DataFrame**

# COMMAND ----------

df_sort = df_csv.sort("ProductKey")

# COMMAND ----------

display(df_sort)

# COMMAND ----------

df_sort2 = df_csv.sort(col("ProductKey"))
display(df_sort2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Filter Data in DataFrame**

# COMMAND ----------

df_csv = (spark.read.format("csv")
              .option("path","/FileStore/tables/Data.csv")
              .option("header", "True").load())
display(df_csv)

# COMMAND ----------

df_flter = df_csv_filter.filter("ProductKey == 310")

# COMMAND ----------

display(df_flter)

# COMMAND ----------

df_flter2 = df_csv_filter.filter("ProducName == 'Road-150'")

# COMMAND ----------

display(df_flter2)

# COMMAND ----------

df_filter3 = df_csv_filter.filter("ProductKey != 310")
display(df_filter3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Remove Duplicates in DataFrame**

# COMMAND ----------

display(df_csv)

# COMMAND ----------

df_dis = df_csv.distinct()
display(df_dis)


# COMMAND ----------

df_dis2 = df_csv.dropDuplicates(["ProducName"])
display(df_dis2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Combine DataFrame**

# COMMAND ----------

df_csv_combine2 = spark.read.csv("/FileStore/tables/EmployeesNew2.csv",header="True")
df_csv_combine = spark.read.csv("/FileStore/tables/Employees.csv",header="True")

display(df_csv_combine) 
display(df_csv_combine2)

# COMMAND ----------

df_combine = df_csv_combine.union(df_csv_combine)
display(df_combine)

# COMMAND ----------

display(df_csv_combine2)

# COMMAND ----------

df_combine2 = df_csv_combine.union(df_csv_combine2)
display(df_combine2)

# COMMAND ----------

df_combine3 = df_csv_combine.unionByName(df_csv_combine2)
display(df_combine3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Fill Null Values in DataFrame**

# COMMAND ----------

df_csvs = (spark.read.format("csv")
          .option("header","true")
          .option("inferSchema","true")
          .option("path","/FileStore/tables/EmployeesNull.csv").load())
display(df_csvs)

# COMMAND ----------

df_csvs.printSchema()

# COMMAND ----------

df_null = df_csvs.na.fill("N/A")
display(df_null)

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

df_nulls = df_csvs.withColumn("id",expr("coalesce(id,0)"))
display(df_nulls)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Pattern Based Filters in DataFrame**

# COMMAND ----------

df_csv = (spark.read.format("csv")
          .option("header","true")
          .option("inferSchema","true")
          .option("path","/FileStore/tables/014_Data.csv").load())
display(df_csv)

# COMMAND ----------

df1 = df_csv.filter("ProductKey == 310")
display(df1)

# COMMAND ----------

df2 = df_csv.where("ProductKey == 310")
display(df2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df3 = df_csv.where(col("ProductName").startswith("A"))
display(df3)

# COMMAND ----------

df4 = df_csv.where(col("ProductName").endswith("e"))
display(df4)

# COMMAND ----------

df5 = df_csv.where(col("ProductName").contains("a"))
display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Add Column on basis of Condition**

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

df_c = df_csv.withColumn("Category",when(col("color") == "Red",1))
display(df_c)

# COMMAND ----------

df_c2 = df_csv.withColumn("Category",when(col("color") == "Red",1).otherwise(0))
display(df_c2)

# COMMAND ----------

df_c3 = df_csv.withColumn("Category",when(col("color") == "Red",1).when(col("Color") == "Black",2)
                          .when(col("color") == "Silver",3).otherwise(0))
display(df_c3)
                          

# COMMAND ----------

# MAGIC %md
# MAGIC **Case Conversion in DataFrame**

# COMMAND ----------

from pyspark.sql.functions import col, lower, initcap, upper

# COMMAND ----------

df_cc = df_csv.withColumn("ProductName", lower(col("ProductName")))
display(df_cc)

# COMMAND ----------

df_cc2 = df_cc.withColumn("ProductName", upper(col("ProductName")))
display(df_cc2)

# COMMAND ----------

df_cc3 = df_csv.withColumn("ProductName", initcap(col("ProductName")))
display(df_cc3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Get Top and Bottom Records from DataFrame**

# COMMAND ----------

df_tb = (spark.read.format("csv")
                  .option("header","true")
                  .option("inferScheme","true")
                  .option("path","/FileStore/tables/Employees.csv").load())
display(df_tb)

# COMMAND ----------

df_tb.show(5)

# COMMAND ----------

dt = df_tb.take(5)
display(dt)

# COMMAND ----------

type(dt)

# COMMAND ----------

rdd = sc.parallelize(dt)

# COMMAND ----------

rdd.toDF()

# COMMAND ----------

df_tb.head()

# COMMAND ----------

df_tb.first()

# COMMAND ----------

df_tb.tail(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Aggregations on DataFrame**

# COMMAND ----------

df_csv = (spark.read.format("csv")
          .option("header","true")
          .option("inferSchema","true")
          .option("path","/FileStore/tables/014_Data.csv").load())
display(df_csv)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(df_csv)

# COMMAND ----------

df_sum = df_csv.select(sum("SalesAmount"))
display(df_sum)

# COMMAND ----------

df_sum = df_csv.select(sum("SalesAmount").alias("TotalSales"))
display(df_sum)

# COMMAND ----------

df_avg = df_csv.select(avg("SalesAmount").alias("AvgSales"))
display(df_avg)

# COMMAND ----------

df_avg = df_csv.select(round(avg("SalesAmount"),2).alias("AvgSales"))
display(df_avg)

# COMMAND ----------

df_count = df_csv.select(count("SalesAmount").alias("CountSales"))
display(df_count)

# COMMAND ----------

df_max = df_csv.select(max("SalesAmount").alias("MaxSales"))
display(df_max)

# COMMAND ----------

df_min = df_csv.select(min("SalesAmount").alias("MinSales"))
display(df_min)

# COMMAND ----------

df_distinct = df_csv.select(countDistinct("Country").alias("DistintCountries"))
display(df_distinct)

# COMMAND ----------

df_expr = df_csv.selectExpr("sum(SalesAmount) as TotalSales")
display(df_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC **Aggregation with GroupBy on DataFrame**

# COMMAND ----------

df_aggG = df_csv.groupBy("Country").sum("SalesAmount")
display(df_aggG)

# COMMAND ----------

df_aggG = df_csv.groupBy("Country").avg("SalesAmount")
display(df_aggG)

# COMMAND ----------

df_aggG = df_csv.groupBy("Country").max("SalesAmount")
display(df_aggG)

# COMMAND ----------

df_aggG = df_csv.groupBy("Country").min("SalesAmount")
display(df_aggG)

# COMMAND ----------

df_aggG = df_csv.groupBy("Country").sum("SalesAmount","TaxAmt")
display(df_aggG)

# COMMAND ----------

df_agg = df_csv.groupBy("Country","ProductName").sum("SalesAmount")
display(df_agg)

# COMMAND ----------

df_agg = df_csv.groupBy("Country").agg(sum("SalesAmount"))
display(df_agg)

# COMMAND ----------

df_agg = df_csv.groupBy("Country").agg(sum("SalesAmount").alias("TotalSales"))
display(df_agg)

# COMMAND ----------

df_agg = df_csv.groupBy("Country").agg(round(avg("SalesAmount"),2).alias("TotalSales"))
display(df_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pivot on DataFrame**

# COMMAND ----------

df_pivot = df_csv.groupBy("ProductName").pivot("Country").sum("SalesAmount")
display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC **Unpivot in Data Frame**

# COMMAND ----------

df_unpivot = df_pivot.unpivot("ProductName",["Australia","India"],"Country","Sales")
display(df_unpivot)

# COMMAND ----------


