from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, when
from os.path import expanduser, join, abspath
import csv
from pyspark.sql.functions import *
from delta.tables import *

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


	spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
	
	spark.sql("use covid")

	spark.sql("select Province_State, sum(Confirmed_Cases) as total_confirmed_cases from healthcare where Province_State != 'Unknown' and Country_Region != 'Unknkown' and last_update_date='2020-10-17' group by Province_State order by total_confirmed_cases desc").show()

	spark.sql("select Country_Region, sum(Confirmed_Cases) as total_confirmed_cases from healthcare2 where Province_State != 'Unknown' and Country_Region != 'Unknkown' and last_update_date=(select max(last_update_date) from healthcare2) group by Country_Region order by total_confirmed_cases desc").show()

	spark.sql("select Country_Region, sum(Recovered_Cases) as Total_Recovered_Cases from healthcare2 where Province_State != 'Unknown' and Country_Region != 'Unknkown' and last_update_date=(select max(last_update_date) from healthcare2) group by Country_Region order by Total_Recovered_Cases desc").show()

	spark.sql("select Country_Region, sum(Active_Cases) as Total_Active_Cases from healthcare2 where Province_State != 'Unknown' and Country_Region != 'Unknkown' and last_update_date=(select max(last_update_date) from healthcare2) group by Country_Region order by Total_Active_Cases desc").show()

	spark.sql("select Country_Region, sum(Deaths) as Total_Deaths from healthcare2 where Province_State != 'Unknown' and Country_Region != 'Unknkown' and last_update_date=(select max(last_update_date) from healthcare2) group by Country_Region order by Total_Deaths desc").show()


	spark.sql("select Country_Region,Province_State,sum(Confirmed_Cases) as Total_Confirmed,sum(Recovered_Cases) as Total_Recovered,sum(Active_Cases) as Total_Active,sum(Deaths) as Total_Deaths from healthcare2 where last_update_date=(select max(last_update_date) from healthcare2) group by Country_Region,Province_State").filter("Country_Region='India'").orderBy("Total_Confirmed").show()

	spark.sql("select Country_Region,Province_State,sum(Confirmed_Cases) as Total_Confirmed_Cases,sum(Recovered_Cases) as Total_Recovered,sum(Active_Cases) as Total_Active,sum(Deaths) as Total_Deaths from healthcare2 where last_update_date=(select max(last_update_date) from healthcare2) and Country_Region='India' group by Country_Region,Province_State order by Total_Confirmed_Cases desc limit 1").show()


	spark.sql("select * from(select Country_Region,sum(Confirmed_Cases), dense_rank() over(order by sum(Confirmed_Cases) desc)rank from healthcare2 where last_update_date=(select max(last_update_date) from healthcare2) group by Country_Region) where rank=3").show()

	spark.sql("select sum(Confirmed_Cases)-(select sum(Confirmed_Cases) from healthcare2 where last_update_date='2020-08-01' and country_region='US') as US_August_Cases from healthcare2 where last_update_date='2020-08-30' and country_region='US'").show()

	spark.sql("select sum(Confirmed_Cases)-(select sum(Confirmed_Cases) from healthcare2 where last_update_date='2020-08-01' and country_region='India') as India_August_Cases from healthcare2 where last_update_date='2020-08-30' and country_region='India'").show()


	spark.sql("select Province_State, Country_Region, sum(Deaths) as Total_Deaths from healthcare2 where Province_State != 'Unknown' and Country_Region != 'Unknown' and last_update_date=(select max(last_update_date) from healthcare2) group by Province_State,Country_Region order by Total_Deaths desc").show()

#query 12 starts

	spark.sql("create or replace view phase1 as select sum(Confirmed_Cases)-(select sum(Confirmed_Cases) from healthcare2 where last_update_date='2020-03-25' and country_region='India') as Total_Confirmed_Cases,sum(Active_Cases)-(select sum(Active_Cases) from healthcare2 where last_update_date='2020-03-25' and country_region='India') as Total_Active_Cases,sum(Recovered_Cases)-(select sum(Recovered_Cases) from healthcare2 where last_update_date='2020-03-25' and country_region='India') as Total_Recovered_Cases,sum(Deaths)-(select sum(Deaths) from healthcare2 where last_update_date='2020-03-25' and country_region='India') as Total_Deaths,if(Last_Update_Date <=> null, 'Phase-1 Total', date(Last_Update_Date)) as Last_Update_Date from healthcare2 where Country_Region='India' and Last_Update_Date='2020-04-14' group by rollup(Last_Update_Date) having Last_Update_Date='Phase-1 Total'")


	spark.sql("select * from phase1").show()

	spark.sql("create or replace view phase2 as select sum(Confirmed_Cases)-(select sum(Confirmed_Cases) from healthcare2 where last_update_date='2020-04-15' and country_region='India') as Total_Confirmed_Cases,sum(Active_Cases)-(select sum(Active_Cases) from healthcare2 where last_update_date='2020-04-15' and country_region='India') as Total_Active_Cases,sum(Recovered_Cases)-(select sum(Recovered_Cases) from healthcare2 where last_update_date='2020-04-15' and country_region='India') as Total_Recovered_Cases,sum(Deaths)-(select sum(Deaths) from healthcare2 where last_update_date='2020-04-15' and country_region='India') as Total_Deaths,if(Last_Update_Date <=> null, 'Phase-2 Total', date(Last_Update_Date)) as Last_Update_Date from healthcare2 where Country_Region='India' and Last_Update_Date='2020-05-03' group by rollup(Last_Update_Date) having Last_Update_Date='Phase-2 Total'")

	spark.sql("select * from phase2").show()

	spark.sql("create or replace view phase3 as select sum(Confirmed_Cases)-(select sum(Confirmed_Cases) from healthcare2 where last_update_date='2020-05-04' and country_region='India') as Total_Confirmed_Cases,sum(Active_Cases)-(select sum(Active_Cases) from healthcare2 where last_update_date='2020-05-04' and country_region='India') as Total_Active_Cases,sum(Recovered_Cases)-(select sum(Recovered_Cases) from healthcare2 where last_update_date='2020-05-04' and country_region='India') as Total_Recovered_Cases,sum(Deaths)-(select sum(Deaths) from healthcare2 where last_update_date='2020-05-04' and country_region='India') as Total_Deaths,if(Last_Update_Date <=> null, 'Phase-3 Total', date(Last_Update_Date)) as Last_Update_Date from healthcare2 where Country_Region='India' and Last_Update_Date='2020-05-17' group by rollup(Last_Update_Date) having Last_Update_Date='Phase-3 Total'")

	spark.sql("select * from phase3").show()

	spark.sql("create or replace view phase4 as select sum(Confirmed_Cases)-(select sum(Confirmed_Cases) from healthcare2 where last_update_date='2020-05-18' and country_region='India') as Total_Confirmed_Cases,sum(Active_Cases)-(select sum(Active_Cases) from healthcare2 where last_update_date='2020-05-18' and country_region='India') as Total_Active_Cases,sum(Recovered_Cases)-(select sum(Recovered_Cases) from healthcare2 where last_update_date='2020-05-18' and country_region='India') as Total_Recovered_Cases,sum(Deaths)-(select sum(Deaths) from healthcare2 where last_update_date='2020-05-18' and country_region='India') as Total_Deaths,if(Last_Update_Date <=> null, 'Phase-4 Total', date(Last_Update_Date)) as Last_Update_Date from healthcare2 where Country_Region='India' and Last_Update_Date='2020-05-31' group by rollup(Last_Update_Date) having Last_Update_Date='Phase-4 Total'")

	spark.sql("select * from phase4").show()

	spark.sql("create view lockdown as select * from phase1 union (select * from phase2) union (select * from phase3) union (select * from phase4) order by Last_Update_Date")
	
	df=spark.sql("select * from lockdown").withColumnRenamed("Last_Update_Date","Lockdown_Phases_India")
	
	df.show()

# query 12 ends

	df = spark.read.option("header","true").csv("file:///home/ak/Desktop/Project/covid/part-00000-0776a0fa-8c82-481d-a804-5c552a0b4ad7-c000.csv")
	
	covidDF = spark.read.table("covid.healthcare2")	

	covidDF.filter("Country_Region
='India'").groupBy("Province_State").max("Case_Fatality_Ratio").withColumnRenamed("max(Case_Fatality_Ratio)","Case_Fatality_Ratio").orderBy("Case_Fatality_Ratio",ascending=False).show(10,False)
	
	covidDF2 = covidDF.withColumn("Last_Update_Month", date_format(to_date("Last_Update_Date", "yyyy-mm-dd"), "MMMM"))
	covidDF2.select("Country_Region","Last_Update_Month","Deaths").groupBy("Country_Region","Last_Update_Month").sum("Deaths").sort(desc("sum(Deaths)")).show(truncate=False)


	covidDF.select("Country_Region","Province_State","Confirmed_Cases","Active_Cases","Recovered_Cases","Deaths").filter("Last_Update_Date = '2020-10-17'").groupBy("Country_Region","Province_State").agg(avg(col("Confirmed_Cases")),avg(col("Active_Cases")),avg(col("Recovered_Cases")),avg(col("Deaths"))).sort(desc("avg(Confirmed_Cases)")).show(truncate=False)


	
	covidDF.select("Case_Fatality_Ratio").agg(avg("Case_Fatality_Ratio")).show()
	covidDF.select("Country_Region","Province_State","Confirmed_Cases","Deaths","Case_Fatality_Ratio").filter(covidDF["Case_Fatality_Ratio"]>2.77).sort(desc("Case_Fatality_Ratio")).show(truncate=False)


	covidDF.select("Country_Region","Incidence_Rate").groupBy("Country_Region").agg({"Incidence_Rate":"max"}).sort(desc("max(Incidence_Rate)")).show(5)


	spark.stop()




