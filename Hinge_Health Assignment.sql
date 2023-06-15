from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, date_format, substring, expr, year

spark = SparkSession.builder.master("local[*]").appName("Assignment_Hinge").getOrCreate()

# Reading all three csv file
unity_golf_club_ct = spark.read.format("csv").option("header", True).load(
    "C:\\Users\\001ZGJ744\\PycharmProjects\\pythonProject"
    "\\data\\unity_golf_club.csv")

unity_golf_club = unity_golf_club_ct.withColumn("source_file", lit("unity_golf_club.csv"))

us_softball_league_ct = spark.read.format("csv").option("header", True).option("delimiter", "\t").load(
    "C:\\Users\\001ZGJ744\\PycharmProjects"
    "\\pythonProject\\data\\us_softball_league"
    ".tsv")

us_softball_league = us_softball_league_ct.withColumn("source_file", lit("us_softball_league.csv"))

companiesData = spark.read.format("csv").option("header", True).load("C:\\Users\\001ZGJ744\\PycharmProjects"
                                                                     "\\pythonProject\\data\\companies.csv")

# Standardize first and last name columns with common date format
Standardize = unity_golf_club.withColumn("Name", concat(col("first_name"), lit(" "), col("last_name"))).select("name",
                                                                                                               date_format(
                                                                                                                   col("dob"),
                                                                                                                   "dd/MM/yyyy").alias(
                                                                                                                   "dob"),
                                                                                                               "company_id",
                                                                                                               date_format(
                                                                                                                   col("last_active"),
                                                                                                                   "dd/MM/yyyy").alias(
                                                                                                                   "last_active"),
                                                                                                               "score",
                                                                                                               "member_since",
                                                                                                               "state","source_file")


# Converting all state into two character
softball_league_state = us_softball_league.withColumn("us_state", substring("us_state", 1, 2))

# Combine the two files
combinedFile = Standardize.unionAll(us_softball_league)

JoinCond = [combinedFile.company_id == companiesData.id]
finalResult = combinedFile.join(companiesData, JoinCond, "inner").select(combinedFile.name, combinedFile.dob,
                                                                         companiesData.name.alias("Company_name"), combinedFile.last_active,
                                                                         combinedFile.score, combinedFile.member_since,
                                                                         combinedFile.state, combinedFile.source_file)

finalResult.show()
# Extracting bad record based on year of birth > year of member_since
BadDataTemp = finalResult.withColumn("yearOfBirth", year(col("dob")))
badDataFinal = BadDataTemp.filter(col("yearOfBirth") > col("member_since"))

# Write bad records into a separate file.
badDataFinal.write.parquet("bada data file path")

# removing bad data from final set of data using left anti join
# Only selecting data from left final table which includes only non-matching record from left table
joinCondForBadRecord = [finalResult.name == badDataFinal.name]

ValidDataWithoutBadData = finalResult.join(badDataFinal, joinCondForBadRecord, "left_anti")
ValidDataWithoutBadData.write.parquet("file path")






