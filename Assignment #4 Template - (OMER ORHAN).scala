// Databricks notebook source
// MAGIC %md ### Spark SQL Template - Assignment #4 - San Francisco Restaurant Inspection Data 
// MAGIC ##### [OMER ORHAN]
// MAGIC ##### General steps
// MAGIC * Create a case class for each data set
// MAGIC * Use CSV reader to read in each data file
// MAGIC * Convert RDD to DataFrame

// COMMAND ----------

// MAGIC %md ### Setting up input data sets

// COMMAND ----------

import org.apache.spark.sql.Encoders
val baseDir = "/FileStore/tables/ASSIGNMENT4"
val raw_inspections = sc.textFile(s"$baseDir/inspections_plus.tsv")
val violations = sc.textFile(s"$baseDir/violations_plus.tsv")
val business = sc.textFile(s"$baseDir/businesses_plus.tsv")

case class Inspection_Plus(business_id_I: Int, score: Int, year:String, Type:String)
var Inspection_PlusSchema = Encoders.product[Inspection_Plus].schema
var Inspection_PlusDf = spark.read.format("csv").     
                option("header", "true").  
                option("delimiter", "\t"). 
                schema(Inspection_PlusSchema).
                load(s"$baseDir/inspections_plus.tsv")

case class Violations_plus(business_id_V: Int, date:String, violationTypeID: Int, risk_category:String, description:String)
var Violations_plusSchema = Encoders.product[Violations_plus].schema
var Violations_plusDf = spark.read.format("csv").     
                option("header", "true").  
                option("delimiter", "\t"). 
                schema(Violations_plusSchema).
                load(s"$baseDir/violations_plus.tsv")

case class Business_plus(business_id_B: Int, name:String, address:String, city:String, postal_code:String, latitude: String, longitude:String,
                         phone_number:String, tax_code:String, business_certificate:String, application_date:String, owner_name:String,owner_address:String, owner_city:String,owner_state:String,owner_zip:String)

var Business_plusSchema = Encoders.product[Business_plus].schema
var Business_plusDf = spark.read.format("csv").     
                option("header", "true").  
                option("delimiter", "\t"). 
                schema(Business_plusSchema).
                load(s"$baseDir/businesses_plus.tsv")


// COMMAND ----------

// MAGIC %md #### 1) What is the inspection score distribution like? (inspections_plus.csv) 
// MAGIC 
// MAGIC Expected output - (***score, count***) - order by score in descending order

// COMMAND ----------

val distrubution= Inspection_PlusDf.groupBy("score").count().orderBy($"score".desc)
distrubution.show

// COMMAND ----------

// MAGIC %md #### 2) What is the risk category distribution like? (violations_plus.csv) 
// MAGIC Expected output - (***risk category, count***)

// COMMAND ----------

val distrubution= Violations_plusDf.groupBy("risk_category").count().orderBy($"risk_category".desc)
distrubution.show

// COMMAND ----------

// MAGIC %md #### 3) Which 20 businesses got lowest scores? 
// MAGIC 
// MAGIC (inspections_plus.csv, businesses_plus.csv)
// MAGIC 
// MAGIC (This should be low score rather than lowest score)
// MAGIC 
// MAGIC Expected columns - (***business_id,name,address,city,postal_code,score***)

// COMMAND ----------

val joinedbusiness = Business_plusDf.join(Inspection_PlusDf, Business_plusDf("business_id_B")=== Inspection_PlusDf("business_id_I"))
joinedbusiness.filter($"score".isNotNull).orderBy($"score".asc).select("business_id_I","name","address","city","postal_code","score").show(20)

// COMMAND ----------

// MAGIC %md #### 4) Which 20 businesses got highest scores? 
// MAGIC 
// MAGIC (inspections_plus.csv, businesses_plus.csv)
// MAGIC 
// MAGIC Expected columns - (***business_id,name,address,city,postal_code,score***)

// COMMAND ----------

val joinedbusiness = Business_plusDf.join(Inspection_PlusDf, Business_plusDf("business_id_B")=== Inspection_PlusDf("business_id_I"))
joinedbusiness.filter($"score".isNotNull).orderBy($"score".desc).select("business_id_B","name","address","city","postal_code","score").show(20)

// COMMAND ----------

// MAGIC %md #### 5) Among all the restaurants that got 100 score, what kind of violations did they get (if any)
// MAGIC (inspections_plus.csv, violations_plus.csv)
// MAGIC 
// MAGIC (Examine "High Risk" violation only)
// MAGIC 
// MAGIC Expected columns - ***(business_id, risk_category, date, description)***
// MAGIC 
// MAGIC Note - format the date in (***month/day/year***)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

Violations_plusDf.createOrReplaceTempView("Violations")
Inspection_PlusDf.createOrReplaceTempView("Inspection")

val sqlDF = spark.sql("SELECT business_id_I, risk_category,date_format( to_date(date,'yyyymmdd'),'MM/DD/YYYY') as date, description FROM Inspection JOIN Violations ON business_id_I = business_id_V where score=100 and risk_category='High Risk'")
sqlDF.show()


// COMMAND ----------

// MAGIC %md #### 6) Average inspection score by zip code
// MAGIC 
// MAGIC (inspections_plus.csv, businesses_plus.csv)
// MAGIC 
// MAGIC Expected columns - (***zip, average score with only two digits after decimal***) - order by average inspection score in descending order

// COMMAND ----------

Business_plusDf.createOrReplaceTempView("Business")
Inspection_PlusDf.createOrReplaceTempView("Inspection")

val sqlDF = spark.sql("SELECT owner_zip, round(avg(score),2) as avg_score from Business JOIN Inspection on business_id_B = business_id_I group by owner_zip order by avg_score desc")
sqlDF.show

// COMMAND ----------

// MAGIC %md #### 7) Compute the proportion of all businesses in each neighborhood that have incurred at least one of the violations
// MAGIC * "High risk vermin infestation"
// MAGIC * "Moderate risk vermin infestation"
// MAGIC * "Sewage or wastewater contamination”
// MAGIC * "Improper food labeling or menu misrepresentation"
// MAGIC * "Contaminated or adulterated food”
// MAGIC * "Reservice of previously served foods"
// MAGIC * "Expected output: zip code, percentage"
// MAGIC 
// MAGIC This question is asking for each neighborhood, what is the proportion of businesses that have incurred at least one of the above nasty violations
// MAGIC 
// MAGIC Note: use UDF to determine which violations match with one of the above extremely bad violations
// MAGIC 
// MAGIC Expected columns - (***zip code, total violation count, extreme violation count, proportion with only two digits after decimal***)
// MAGIC 
// MAGIC Order the result by the proportion in descending order

// COMMAND ----------

Business_plusDf.createOrReplaceTempView("Business")
Violations_plusDf.createOrReplaceTempView("Violations")

def detechViolations(x: String) = if (x == "High risk vermin infestation" 
                                      || x == "Moderate risk vermin infestation"
                                     || x == "Sewage or wastewater contamination"
                                     || x == "Improper food labeling or menu misrepresentation"
                                     || x == "Contaminated or adulterated food"
                                      || x == "Reservice of previously served foods"
                                       || x == "Expected output: zip code, percentage"
                                     ) "1" else "0"

spark.udf.register("detechViolations",detechViolations(_:String))

//get extreme_count with UDF
//val sqlDF3 = spark.sql("select owner_zip as extreme_owner_zip, count(*) as extreme_count from Business JOIN Violations on business_id_V=business_id_B where detechViolations(description)='1' group by owner_zip")

//get total_violation_count
//val sqlDF2 = spark.sql("select owner_zip as general_owner_zip, count(*) as total_violation_count from Business group by owner_zip")

val sqlDF4 = spark.sql("select general_owner_zip,total_violation_count,extreme_count,round((extreme_count/total_violation_count),2) as proportion  from (select owner_zip as extreme_owner_zip, count(*) as extreme_count from Business JOIN Violations on business_id_V=business_id_B where detechViolations(description)='1' group by owner_zip) JOIN (select owner_zip as general_owner_zip, count(*) as total_violation_count from Business group by owner_zip) on general_owner_zip=extreme_owner_zip")

sqlDF4.show



// COMMAND ----------

// MAGIC %md ### 8) Are SF restaurants clean? Justify your answer
// MAGIC 
// MAGIC (***Make to sure backup your answer with data - don't just state your opinion***)

// COMMAND ----------

// MAGIC %md (Yes/No) and why
// MAGIC Answer;

// COMMAND ----------

// (Yes/No) and why
//Answer; YES

//SF restaurants are clean. Because;
//1-When I look the inspection table, most of businesses got over 90 scores. That is very good.
//+-----+-----+
//|score|count|
//+-----+-----+
//|  100| 3705|
//|   98| 1534|
//|   96| 2365|
//|   94| 1751|
//|   93|  374|
//|   92| 1482|
//|   91|  411|
//|   90| 1241|

//2- High Risk Count is pretty low than Moderate and Low Risk. That is an evidence showing SF restaurants are clean.
//risk_category|count|
//+-------------+-----+
//|          N/A|   31|
//|Moderate Risk|15712|
//|     Low Risk|24717|
//|    High Risk| 6446|



// COMMAND ----------

// MAGIC %md ### 9) Extra Credit - Plot the businesses with lowest score and high scores on map
// MAGIC <h4>Resources</h4>
// MAGIC <ul>
// MAGIC   <li><a href="https://www.latlong.net/search.php?keyword=San+Francisco">Lat/long locator</a></li>
// MAGIC   <li><a href="https://leafletjs.com/">Open source JS library for interactive map</a></li>
// MAGIC </ul>

// COMMAND ----------

val lowScoreData = List(List(37.783268, -122.410553, "Tip Top Market"), List(37.743103, -122.473899, "ABC Bakery"))

// COMMAND ----------

val lowScoreStr = lowScoreData.map(r => s"""L.circle([${r(0)}, ${r(1)}], {color: 'red',fillColor: '#f03',fillOpacity: 0.5,radius: 100}).bindTooltip("${r(2)}").addTo(mymap);""").mkString("\n")

// COMMAND ----------

val highScoreData = List(List(37.787386, -122.433476, "My House"), List(37.782673, -122.469239, "Tong Place"))

// COMMAND ----------

val highScoreStr = highScoreData.map(r => s"""L.circle([${r(0)}, ${r(1)}], {color: 'green',fillColor: '#86FF33',fillOpacity: 0.5,radius: 100}).bindTooltip("${r(2)}").addTo(mymap);""").mkString("\n")

// COMMAND ----------

val combinedCirclesStr = lowScoreStr + "\n" + highScoreStr

// COMMAND ----------

// https://www.srihash.org/ for generating the integrity 
displayHTML("""
<html>
<head>

 <link rel="stylesheet" href="https://unpkg.com/leaflet@1.3.1/dist/leaflet.css" integrity="sha384-odo87pn1N9OSsaqUCAOYH8ICyVxDZ4wtbGpSYO1oyg6LxyOjDuTeXTrVLuxUtFzv" crossorigin="">
 
 <script src="https://unpkg.com/leaflet@1.3.1/dist/leaflet.js" integrity="sha384-JguaQYjdUVs0XHRhkpHTNjd6j8IjEzgj5+1xeGYHCXQEaufKsYdtn9fgHVeVTLJu" crossorigin="anonymous"></script>
 
 <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>
</head>
<body>
    <div><center><h3>Powered by <a href="https://leafletjs.com/examples/quick-start/" target="_">Leaf Let</a></center></h3></div>
    <div id="mapid" style="width:900px; height:600px"></div>
  <script>
  var mymap = L.map('mapid').setView([37.7587,-122.4486], 13);
  var tiles = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
}).addTo(mymap); """ + combinedCirclesStr + """
  </script>
  </body>
  </html>
""")
