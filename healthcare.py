from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, when
from os.path import expanduser, join, abspath
import csv
from pyspark.sql.functions import *
from delta.tables import *



# converting various timestamp format to standard format

def to_date_(col, formats=("yyyy-MM-dd'T'HH:mm:ss","yyyy-MM-dd HH:mm:ss","MM/dd/yyyy HH:mm","MM/dd/yy HH:mm","yyyy-MM-dd'T'HH:mm:ss","yyyy-MM-dd HH:mm:ss")):

	return coalesce(*[to_timestamp(col, f) for f in formats])

#replacing empty string with Unknown

def blank_as_null(spark,x):
	return when(col(x) != "", col(x)).otherwise("Unknown")



if __name__ == "__main__":

	warehouse_location = abspath('spark-warehouse')

	spark = SparkSession \
	.builder \
	.appName("Healthcare COVID-19") \
	.config("spark.sql.warehouse.dir", warehouse_location) \
	.enableHiveSupport() \
 	.config("spark.jars.packages", "io.delta:delta-core_2.11:0.4.0") \
    	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
	.getOrCreate()

	
	# reading tables from hive to pyspark data frame and lisiting string type columns

	covid1DF = spark.read.table("covid.covid1")
	covid2DF = spark.read.table("covid.covid2")  
	covid3DF = spark.read.table("covid.covid3")
	covid4DF = spark.read.table("covid.covid4")
	l1 = ["Province_State","Country_Region","Last_Update"]
	l2 = ["Province_State","Country_Region","Last_Update"]
	l3 = ["admin2","province_state","country_region","last_update","combined_key"]
	l4 = ["admin2","province_state","country_region","last_update","combined_key"]


	# replacing empty string to Unknown

	for i in l1:		
		covid1_clean = covid1DF.withColumn(i,blank_as_null(spark,i))
	for i in l2:
		covid2_clean = covid1DF.withColumn(i,blank_as_null(spark,i))
	for i in l3:
		covid3_clean = covid1DF.withColumn(i,blank_as_null(spark,i))
	for i in l4:
		covid4_clean = covid1DF.withColumn(i,blank_as_null(spark,i))

	covid1_clean.show(100)
	covid2_clean.show(100)
	covid3_clean.show(100)
	covid4_clean.show(100)

	# dropping id column 
	# distinct count of rows in each table
	
	covid1_clean = covid1_clean.drop("id")
	coivd1 = covid1_clean.distinct()

	covid2_clean = covid2_clean.drop("id")
	covid2 = covid2_clean.distinct()

	covid3_clean = covid3_clean.drop("id")
	covid3 = covid3_clean.distinct()

	covid4_clean = covid4_clean.drop("id")
	covid4 = covid4_clean.distinct()

	count_covid1 = str(covid1.count())
	count_covid2 = str(covid2.count())
	count_covid3 = str(covid3.count())
	count_covid4 = str(covid4.count())


	# writing distinct count to txt file
	
    output_file  = open("/home/ak/Desktop/Project/output.txt", "a")
    output_file .write("Distinct count of each table\n")
    output_file .write(count_covid1+'\n')
    output_file .write(count_covid2+'\n')
    output_file .write(count_covid3+'\n')
    output_file .write(count_covid4+'\n')
    output_file .close()

	# changing various timestamp format to to standard, splitting timestamp to date and time column and type casting

	covid1 = covid1.withColumn("last_update",to_date_("last_update")).withColumn('last_update', regexp_replace('last_update', '0020', '2020')).withColumn("last_update_day", split(col("last_update"), " ").getItem(0)).withColumn("last_update_time", split(col("last_update")," ").getItem(1))
	
	covid1 = covid1.withColumn("last_update_day",covid1['last_update_day'].cast(DateType()))
	

	covid2 = covid2.withColumn("last_update",to_date_("last_update")).withColumn('last_update', regexp_replace('last_update', '0020', '2020')).withColumn("last_update_day", split(col("last_update"), " ").getItem(0)).withColumn("last_update_time", split(col("last_update")," ").getItem(1))
	
	covid2 = covid2.withColumn("last_update_day",covid2['last_update_day'].cast(DateType()))


	covid3 = covid3.withColumn("last_update",to_date_("last_update")).withColumn('last_update', regexp_replace('last_update', '0020', '2020')).withColumn("last_update_day", split(col("last_update"), " ").getItem(0)).withColumn("last_update_time", split(col("last_update")," ").getItem(1))

	covid3 = covid3.withColumn("last_update_day",covid3['last_update_day'].cast(DateType()))


	covid4 = covid4.withColumn("last_update",to_date_("last_update")).withColumn('last_update', regexp_replace('last_update', '0020', '2020')).withColumn("last_update_day", split(col("last_update"), " ").getItem(0)).withColumn("last_update_time", split(col("last_update")," ").getItem(1))
	covid4 = covid4.withColumn("last_update_day",covid4['last_update_day'].cast(DateType()))



	# renaming columns

	covid1 = covid1.withColumnRenamed("province_state", "Province_State").withColumnRenamed("country_region","Country_Region").withColumnRenamed("last_update","Last_Update_Timestamp").withColumnRenamed("confirmed","Confirmed_Cases").withColumnRenamed("deaths","Deaths").withColumnRenamed("recovered","Recovered_Cases").withColumnRenamed("last_update_day","Last_Update_Date").withColumnRenamed("last_update_time","Last_Update_Time")

	covid2 = covid2.withColumnRenamed("province_state", "Province_State").withColumnRenamed("country_region","Country_Region").withColumnRenamed("last_update","Last_Update_Timestamp").withColumnRenamed("confirmed","Confirmed_Cases").withColumnRenamed("deaths","Deaths").withColumnRenamed("recovered","Recovered_Cases").withColumnRenamed("last_update_day","Last_Update_Date").withColumnRenamed("last_update_time","Last_Update_Time").withColumnRenamed("latitude","Latitude").withColumnRenamed("longitude","Longitude")

	covid3 = covid3.withColumnRenamed("province_state", "Province_State").withColumnRenamed("country_region","Country_Region").withColumnRenamed("last_update","Last_Update_Timestamp").withColumnRenamed("confirmed","Confirmed_Cases").withColumnRenamed("deaths","Deaths").withColumnRenamed("recovered","Recovered_Cases").withColumnRenamed("last_update_day","Last_Update_Date").withColumnRenamed("last_update_time","Last_Update_Time").withColumnRenamed("latitude","Latitude").withColumnRenamed("longitude","Longitude").withColumnRenamed("fips","FIPS").withColumnRenamed("admin2","Admin").withColumnRenamed("active","Active_Cases").withColumnRenamed("combined_key","Combined_Key")

	covid4 = covid4.withColumnRenamed("province_state", "Province_State").withColumnRenamed("country_region","Country_Region").withColumnRenamed("last_update","Last_Update_Timestamp").withColumnRenamed("confirmed","Confirmed_Cases").withColumnRenamed("deaths","Deaths").withColumnRenamed("recovered","Recovered_Cases").withColumnRenamed("last_update_day","Last_Update_Date").withColumnRenamed("last_update_time","Last_Update_Time").withColumnRenamed("latitude","Latitude").withColumnRenamed("longitude","Longitude").withColumnRenamed("fips","FIPS").withColumnRenamed("admin2","Admin").withColumnRenamed("active","Active_Cases").withColumnRenamed("combined_key","Combined_Key").withColumnRenamed("incidence_rate","Incidence_Rate").withColumnRenamed("case_fatality_ratio","Case_Fatality_Ratio")

	covid1.show(10)
	covid1.printSchema()

	covid2.show(10)
	covid2.printSchema()

	covid3.show(10)
	covid3.printSchema()

	covid4.show(10)
	covid4.printSchema()
	
	# writing data frames as delta table and schema merge
	
	covid1.write.format("delta").save("file:///home/ak/Desktop/Project/delta/covid")
	covid2.write.format("delta").mode("append").option("mergeSchema","true").save("file:///home/ak/Desktop/Project/delta/covid")
	covid3.write.format("delta").mode("append").option("mergeSchema","true").save("file:///home/ak/Desktop/Project/delta/covid")
	covid4.write.format("delta").mode("append").option("mergeSchema","true").save("file:///home/ak/Desktop/Project/delta/covid")


	spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
	spark.sql("use covid")
	spark.sql("create table covid_delta using delta location 'file:///home/ak/Desktop/Project/delta/covid'")
	
	covid_final = spark.read.table("covid.covid_delta")
	covid_final  = covid_final .select("FIPS","Admin","Country_Region","Province_State","Latitude","Longitude","Last_Update_Date","Last_Update_Time","Last_Update_Timestamp","Confirmed_Cases","Active_Cases","Deaths","Recovered_Cases","Incidence_Rate","Case_Fatality_Ratio","Combined_Key")
	covid_final  = covid_final .withColumn("Admin",blank_as_null(spark,"Admin"))
	covid_final  = covid_final .withColumn('Combined_Key', concat(col('Admin'),lit(' '), col('Province_State'),lit(' '), col('Country_Region')))	
	
	covid_final .printSchema()
	
	count_merge=str(cfinal.count())
	

	#saving file in different formats
	
	covid_final .coalesce(1).write.option("header","true").csv("file:///home/ak/Desktop/Project/data/healthcare_final")
	covid_final .coalesce(1).write.option("header","true").csv("hdfs://localhost:9000/covid_data/healthcare")
	covid_final .write.parquet("hdfs://localhost:9000/user/hive/warehouse/covid.db/healthcare_parquet")
	covid_final .write.option("codec","bzip2").csv("hdfs://localhost:9000/user/hive/warehouse/covid.db/healthcare_bzip")
	covid_final .write.option("codec","gzip").csv("hdfs://localhost:9000/user/hive/warehouse/covid.db/healthcare_gzip")
	
	# partiton and bucketing
	
	covid_final .write.partitionBy("Country_Region","Province_State",).save("file:///home/ak/Desktop/Project/data/partby_country_province")
	covid_final .write.partitionBy("Country_Region").bucketBy(10,"Province_State").saveAsTable("covid.covidfbucket")
	
	
	# writing count of final merged schema to output file
	output_file = open("/home/ak/Desktop/Project/output.txt", "a")
	output_file.write("Distinct count of merged schema\n")
	output_file.write(count_merge+'\n')
	output_file.close()

	spark.stop()
