-- Databricks notebook source
SELECT * 
FROM csv.`/Volumes/workspace/retail/mylocalfiles/Products_CSV.csv`

-- COMMAND ----------

-- DBTITLE 1,Untitled
CREATE OR REPLACE TABLE Products AS
SELECT 
  ProductID AS product_id,
  `Product Name` AS product_name,
  Category AS category,
  SubCategory AS subcategory,
  `Unit Price (MRP)` AS unit_price_mrp
FROM read_files(
  '/Volumes/workspace/retail/mylocalfiles/Products_CSV.csv',
  format => 'csv',
  header => true
);

SELECT * FROM Products;


-- COMMAND ----------

CREATE OR REPLACE TABLE Products AS
SELECT 
  *
FROM read_files(
  '/Volumes/workspace/retail/mylocalfiles/Products_CSV.csv',
  format => 'csv',
  header => true,
  schema => 'product_id STRING, 
  product_name STRING, 
  category STRING, 
  subcategory STRING, 
  unit_price_mrp DOUBLE
  '
);

SELECT * FROM Products;


-- COMMAND ----------


ALTER TABLE workspace.default.products RENAME TO workspace.retail.products;



-- COMMAND ----------

SELECT *
FROM csv.`/Volumes/workspace/retail/mylocalfiles/Sales_CSV.csv`

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA retail;

CREATE OR REPLACE TABLE sales (
    OrderID STRING NOT NULL,
    CustomerID STRING,
    OrderDate DATE,
    ProductID STRING,
    Discount DOUBLE,
    Profit INT,
    Quantity INT,
    ShipDate DATE,
    ShippingID STRING,
    Price DOUBLE
)


-- COMMAND ----------

COPY INTO sales
FROM '/Volumes/workspace/retail/mylocalfiles/Sales_CSV.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS('header'= 'true', 'inferschema'='true')

-- COMMAND ----------

SELECT * 
FROM sales

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA retail;

CREATE TABLE yearly_sales (
    OrderID STRING NOT NULL,
    CustomerID STRING,
    ProductID STRING
)
USING DELTA;

-- COMMAND ----------

COPY INTO yearly_sales
FROM '/Volumes/workspace/retail/mylocalfiles/yearly_sales/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema' = 'true')

-- COMMAND ----------

SELECT * FROM yearly_sales

-- COMMAND ----------

Select * FROM sales

-- COMMAND ----------

CREATE TABLE shipping (
    ShippingId STRING,
    Ship_Mode STRING,
    Shipping_Cost DECIMAL(10,2),
    Carrier STRING
)
USING DELTA;

-- COMMAND ----------

INSERT INTO shipping (ShippingId, Ship_Mode, Shipping_Cost, Carrier)
VALUES ('S001', 'Economy', 0.0, 'FedEx');

INSERT INTO shipping (ShippingId, Ship_Mode, Shipping_Cost, Carrier)
VALUES ('S002', 'Economy Plus', 10.0, 'UPS');

INSERT INTO shipping (ShippingId, Ship_Mode, Shipping_Cost, Carrier)
VALUES ('S003', 'Immediate', 25.0, 'DHL');

INSERT INTO shipping (ShippingId, Ship_Mode, Shipping_Cost, Carrier)
VALUES ('S004', 'Priority', 30.0, 'DHL');


-- COMMAND ----------

SELECT * FROM shipping

-- COMMAND ----------

DESCRIBE DETAIL shipping

-- COMMAND ----------

SELECT * FROM shipping

INSERT INTO shipping (ShippingId, Ship_Mode, Shipping_Cost, Carrier)
VALUES ('S005', 'Economy', 0.0, 'FedEx');

-- COMMAND ----------

UPDATE shipping SET Shipping_Cost = 20.0 WHERE ShippingId = 'S005'

-- COMMAND ----------

SELECT * FROM shipping

-- COMMAND ----------

DELETE FROM shipping 
where ShippingId = 'S005'

-- COMMAND ----------

DESCRIBE HISTORY shipping

-- COMMAND ----------

SELECT * FROM shipping VERSION AS OF 7

-- COMMAND ----------

INSERT INTO shipping VALUES('S006', 100.0)

-- COMMAND ----------

ALTER TABLE shipping
ADD COLUMN comments STRING

DESCRIBE TABLE shipping



-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA cloud_ingestion;

CREATE TABLE s3_targetsales
USING DELTA
LOCATION 's3://amzn-s3-databricksdemo/delta-output/'
AS
SELECT *
FROM read_files(
  's3://amzn-s3-databricksdemo/raw/Target Sales.csv',
  format => 'csv',
  header => true
);

-- COMMAND ----------

DESCRIBE EXTENDED s3_targetsales