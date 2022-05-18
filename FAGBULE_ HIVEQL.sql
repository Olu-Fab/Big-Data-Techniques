-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/FileStore/tables/HiveClinicalTrial')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if dbutils.fs.ls('/FileStore/tables/HiveClinicalTrial') == []:
-- MAGIC     print("File Exists")
-- MAGIC else:
-- MAGIC     dbutils.fs.rm('/FileStore/tables/HiveClinicalTrial', True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('FileStore/tables/clinicaltrial_2021.csv', "FileStore/tables/HiveClinicalTrial/clinicaltrial_2021.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/HiveClinicalTrial")

-- COMMAND ----------

--Question 1
--Create clinical Trial table using spark options

CREATE TABLE IF NOT EXISTS clinicaltrial_2021(
Id string,
Sponsor string,
Status string,
Start string,
Completion string,
Type string,
Sybmission string,
Conditions string,
Interventions string)
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/HiveClinicalTrial",
        delimiter "|",
        header "true")
        ;

-- COMMAND ----------

--Select Top 5 rows from the clinicalTrial Table
SELECT * FROM clinicaltrial_2021 LIMIT 5;

-- COMMAND ----------

--Count Number of Studies in the clinicalTrial Table
SELECT count(DISTINCT Id) AS Studies_Number FROM clinicaltrial_2021;

-- COMMAND ----------

--Question 2
SELECT Type, count(Type) as Frequency from clinicaltrial_2021 
GROUP BY Type
ORDER BY count (Type) desc;

-- COMMAND ----------

--Question 3
--Select Top 5 Conditions
SELECT conditions, count(*) AS Frequency
FROM (SELECT explode(split(conditions,','))AS Conditions
      FROM clinicaltrial_2021)
WHERE conditions != ''
GROUP BY conditions
ORDER BY count(*) DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/FileStore/tables/HiveMesh')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('FileStore/tables/mesh.csv', 'FileStore/tables/HiveMesh/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('/FileStore/tables/HiveMesh/')

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS mesh(
Term string,
Tree string)
USING CSV
OPTIONS (path "/FileStore/tables/HiveMesh",
        delimiter ",",
        header "true")



-- COMMAND ----------

select * from mesh limit 5;

-- COMMAND ----------

SELECT REPLACE (substring(tree, 1, 3), '"',"") AS Root, count(conditions) AS Frequency
FROM mesh
INNER JOIN (SELECT explode(split(conditions,','))AS conditions
       FROM clinicaltrial_2021)
ON term = conditions
WHERE conditions != ''
GROUP BY REPLACE(substring(tree, 1, 3), '"',"")
ORDER BY count(conditions)DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/FileStore/tables/HivePharma')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('FileStore/tables/pharma.csv', "FileStore/tables/HivePharma")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/HivePharma")

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS pharma(
Company string,
Parent_Company string,
Penalty_Amount string,
Subtraction_From_Penalty string,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting string,
Penalty_Year string,
Penalty_Date string,
Offense_Group string,
Primary_Offense string,
Secondary_Offense string,
Description string,
Level_of_Government string,
Action_Type string,
Agency string,
Civil_Criminal string,
Prosecution_Agreement string,
Court string,
Case_ID string,
Private_Litigation_Case_Title string,
Lawsuit_Resolution string,
Facility_State string,
City string,
Address string,
Zip string,
NAICS_Code string,
NAICS_Translation string,
HQ_Country_of_Parent string,
HQ_State_of_Parent string,
Ownership_Structure string,
Parent_Company_Stock_Ticker string,
Major_Industry_of_Parent string,
Specific_Industry_of_Parent string,
Info_Source string,
Notes string
)
row format delimited
fields terminated by ","
location "/FileStore/tables/HivePharma"

-- COMMAND ----------

select * from pharma limit 5

-- COMMAND ----------

SELECT sponsor, count(id) AS Number_Trials
FROM clinicaltrial_2021
LEFT JOIN pharma
ON REPLACE(parent_company, '"',"") = Sponsor
WHERE REPLACE (parent_company, '"', "") IS NULL
GROUP BY Sponsor
ORDER BY count(id) DESC
LIMIT 10;

-- COMMAND ----------

SELECT YEAR(FROM_UNIXTIME(unix_timestamp()))*100 + MONTH(FROM_UNIXTIME(unix_timestamp()))

-- COMMAND ----------

--Number of Completed Studies for each Month in Year 2021
SELECT substring(completion, 1,3) AS Month, count(substring(completion, 1,3)) AS Studies
FROM clinicaltrial_2021
WHERE status="Completed" AND substring(completion, 5,4) = 2021
GROUP BY substring(completion, 1,3)
ORDER BY 'Month' DESC
