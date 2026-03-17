-- Lecture: Writing Basic Queries in Databricks
-- Purpose: Demonstrate Basic SQL Queries


SHOW CATALOGS
SHOW Schemas

SELECT ID, Email, City
FROM retail.customers
LIMIT 10

SELECT * 
FROM Customers
WHERE Region = 'North'

SELECT * 
FROM Customers
WHERE Region NOT IN ('North', 'South')

SELECT *
FROM Customers
WHERE City = 'Frankfurt' AND Region = 'Central'

SELECT *
FROM Customers
WHERE City = 'Frankfurt' OR Region = 'Central'


---------------------------------------------------------

-- Lecture: Aggregate functions, GROUP BY and HAVING, String, Date, and Data Conversion Functions
-- Purpose: Demonstrate SQL functions

SELECT *
FROM sales;

SELECT COUNT(DISTINCT CustomerID) AS `No Of customers`,
       approx_count_distinct(CustomerID) AS `Approx no of customers`
FROM sales;

SELECT MIN(Profit) AS MinProfit, MAX(Profit) AS MaxProfit, AVG(Profit) AS AvgProfit
FROM sales;


SELECT * FROM sales;

SELECT CustomerID, SUM(Profit) AS TotalProfit
FROM sales
GROUP BY CustomerID
HAVING SUM(Profit) < 0;

--String functions

SELECT * FROM customers;

SELECT `First Name`, LEN(`First Name`), UPPER(`First Name`), lower(`First Name`)
FROM customers
LIMIT 10;

SELECT ltrim('     hello'), rtrim('hello     '), trim('     hello     ');

SELECT SUBSTRING('Hello World', 7, 4);
SELECT REPLACE('Hello_World', 'World', 'Universe'); 

SELECT concat(`First Name`, ' ', `Last Name`) AS Full_Name
FROM customers
LIMIT 10;


-- Date functions
SELECT * FROM sales;

SELECT OrderDate, ShipDate, DATEDIFF(ShipDate, OrderDate) AS DaysToShip
FROM sales
LIMIT 100;

SELECT OrderDate, QUARTER(OrderDate) AS Quarter, YEAR(OrderDate) AS Year, MONTH(OrderDate) AS Month
FROM sales
LIMIT 1000;

SELECT date_format(OrderDate, 'MMMM') AS MonthName, date_format(OrderDate, 'MM') AS MonthNumber
FROM sales
LIMIT 1000;

-- Data Conversion Functions

SELECT CAST('123' AS INT);
SELECT CAST('2023-06-17' AS DATE);

SELECT TRY_CAST('abc' AS INT);

SELECT 
  typeof(Profit) AS before_type,
  typeof(CAST(Profit AS STRING)) AS after_type
FROM sales
LIMIT 1000;


---------------------------------------------------------

-- Lecture: Combining Tables Using Joins in Databricks SQL
-- Purpose: Demonstrate all join types

SELECT * FROM country_sales;
SELECT * FROM country_target;

SELECT 
    s.Country,
    s.Sales_Recorded,
    t.Target_Sales
FROM country_sales s
INNER JOIN country_target t
ON s.Country = t.Country;



SELECT 
    s.Country,
    s.Sales_Recorded,
    t.Country,
    t.Target_Sales
FROM country_sales s
LEFT JOIN country_target t
ON s.Country = t.Country;


SELECT 
    s.Country,
    s.Sales_Recorded,
    t.Country,
    t.Target_Sales
FROM country_sales s
RIGHT JOIN country_target t
ON s.Country = t.Country;


SELECT 
    s.Country,
    s.Sales_Recorded,
    t.Country,
    t.Target_Sales
FROM country_sales s
FULL OUTER JOIN country_target t
ON s.Country = t.Country;


SELECT *
FROM country_sales s
LEFT SEMI JOIN country_target t
ON s.Country = t.Country;



SELECT *
FROM country_sales s
LEFT ANTI JOIN country_target t
ON s.Country = t.Country;



CREATE TABLE Tshirt (
    Color STRING
)
USING DELTA;

INSERT INTO Tshirt VALUES
('Red'),
('Green'),
('Blue'),
('White');


CREATE TABLE MasterSize (
    Size STRING
)
USING DELTA;

INSERT INTO MasterSize VALUES
('Small'),
('Medium'),
('Large');


SELECT * FROM tshirt; 
SELECT * FROM mastersize;

SELECT * FROM tshirt CROSS JOIN mastersize;


---------------------------------------------------------

-- Lecture: Combining Result Sets Using Set Operators in Databricks SQL
-- Purpose: Demonstrate Union, Union all, Intersect, Except


SELECT * FROM country_sales;
SELECT * FROM country_target;

SELECT Country, Sales_Recorded
FROM country_sales
UNION
SELECT Country, Target_Sales
FROM country_target;

SELECT Country
FROM country_sales
UNION ALL
SELECT Country
FROM country_target;


SELECT Country
FROM country_sales
INTERSECT
SELECT Country
FROM country_target;


SELECT Country
FROM country_target
EXCEPT
SELECT Country
FROM country_sales;


---------------------------------------------------------

-- Lecture: Handling NULL, Case Expressions, Subqueries, CTE, Window functions in Databricks SQL
-- Purpose: Demonstrate concepts for NULL, Case Expressions, Subqueries, CTE, Window functions


CREATE TABLE students (
    StudentID INT,
    FirstName STRING,
    LastName STRING,
    Branch STRING,
    Email STRING,
    PhoneNo STRING,
    MarksObtained INT
)
USING DELTA;

INSERT INTO students VALUES
(1, 'Aarav', 'Kumar', 'ECE', 'aarav.kumar@example.com', '1234567890', 800),
(2, 'Aditi', 'Sharma', 'CS', 'aditi.sharma@example.com', NULL, 900),
(3, 'Advait', 'Gupta', 'ECE', NULL, '9876543210', 920),
(4, 'Aishwarya', 'Singh', 'ME', 'aishwarya.singh@example.com', '9988776655', 750),
(5, 'Akash', 'Verma', 'IT', NULL, NULL, 820),
(6, 'Ananya', 'Choudhary', 'ECE', 'ananya.choudhary@example.com', '9090909090', 930),
(7, 'Aniket', 'Yadav', 'CS', 'aniket.yadav@example.com', NULL, 850),
(8, 'Anisha', 'Shah', 'EE', NULL, '9999999999', 780),
(9, 'Anuj', 'Gupta', 'ME', 'anuj.gupta@example.com', '8888888888', 890),
(10, 'Arjun', 'Rao', 'IT', NULL, NULL, 760);


SELECT *
FROM students;

SELECT FirstName, coalesce(Email, PhoneNo, 'No Contact Info') AS Contact_Info
FROM students;

SELECT FirstName, Email, isnull(Email) AS Has_Email
FROM students;

SELECT FirstName, Email, PhoneNo, ifnull(Email, PhoneNo) AS contact
FROM students;

---------------

SELECT * 
FROM products;

SELECT product_id, product_name, category,
  CASE category
    WHEN 'Office Supplies' THEN 'OS'
    WHEN 'Furniture' THEN 'FUR'
    WHEN 'Technology' THEN 'TECH'
    ELSE 'Other'
  END AS category_code
FROM workspace.retail.products;


SELECT product_id, product_name, unit_price_mrp,
  CASE 
    WHEN unit_price_mrp < 20 THEN 'Low'
    WHEN unit_price_mrp < 50 THEN 'Medium'
    WHEN unit_price_mrp < 100 THEN 'High'
    ELSE 'Very High'
  END AS price_band
FROM products;


------------------------

SELECT *
FROM students;

SELECT *
FROM students
WHERE MarksObtained = (SELECT MAX(MarksObtained)
                        FROM students);



SELECT * 
FROM students
WHERE MarksObtained = (
                        SELECT MAX(MarksObtained)
                        FROM students
                        WHERE MarksObtained <> (SELECT MAX(MarksObtained)
                                                FROM students));


SELECT * 
FROM students 
WHERE Branch IN (
                SELECT Branch
                FROM students
                GROUP BY Branch
                HAVING AVG(MarksObtained) > 850);

----------------------

WITH students_details (firstname, lastname, marksobtained) AS(
  SELECT FirstName, LastName, MarksObtained
  FROM Students
  WHERE MarksObtained > 800
)
SELECT firstname, lastname, marksobtained
FROM students_details;

-----------------------

INSERT INTO students VALUES
(11, 'Bhavya', 'Mehta', 'CS', 'bhavya.mehta@example.com', '9123456780', 870),
(12, 'Chirag', 'Patel', 'ECE', NULL, '9012345678', 910),
(13, 'Diya', 'Nair', 'IT', 'diya.nair@example.com', NULL, 845),
(14, 'Eshan', 'Malhotra', 'ME', 'eshan.malhotra@example.com', '9345678901', 790),
(15, 'Falguni', 'Desai', 'EE', NULL, '9234567890', 880),
(16, 'Gaurav', 'Bansal', 'CS', 'gaurav.bansal@example.com', NULL, 940),
(17, 'Harini', 'Iyer', 'ECE', 'harini.iyer@example.com', '9567890123', 860),
(18, 'Ishaan', 'Kapoor', 'IT', NULL, NULL, 905),
(19, 'Jhanvi', 'Arora', 'ME', 'jhanvi.arora@example.com', '9789012345', 815),
(20, 'Kunal', 'Sethi', 'EE', NULL, '9871234567', 895);


SELECT FirstName, LastName, Branch, AVG(MarksObtained) OVER(PARTITION BY Branch) as avg_marks
FROM students;

SELECT FirstName, LastName, Branch, MarksObtained, RANK() OVER(ORDER BY MarksObtained DESC) AS student_rank
FROM students;

SELECT AVG(MarksObtained)
FROM Students;

-- LAG and LEAD

SELECT FirstName, LastName, Branch, MarksObtained, LAG(MarksObtained) OVER(PARTITION BY Branch ORDER BY MarksObtained) AS prev_marks, 
                                           LEAD(MarksObtained) OVER(PARTITION BY Branch ORDER BY MarksObtained) AS next_marks
FROM Students


---------------------------------------------------------

-- Lecture: Views in Databricks SQL
-- Purpose: Demonstrate Standard, Temporary, Materialized, Dynamic Views

SELECT * 
FROM students;

CREATE OR REPLACE VIEW cs_ece_top_students_vw AS
SELECT
  StudentID, FirstName, LastName, Branch, MarksObtained
FROM students
WHERE Branch IN ('CS', 'ECE')
AND MarksObtained > 800;

SELECT * FROM cs_ece_top_students_vw;

INSERT INTO students VALUES
(21, 'Rohan', 'Mehra', 'CS', 'rohan.mehra@example.com', NULL, 910);

INSERT INTO students VALUES
(22, 'Priya', 'Sharma', 'ME', 'priya.sharma@example.com', NULL, 950);

SHOW VIEWS;
DESCRIBE TABLE EXTENDED cs_ece_top_students_vw;


CREATE OR REPLACE TEMP VIEW cs_ece_top_students_temp_vw AS
SELECT
  StudentID, FirstName, LastName, Branch, MarksObtained
FROM students
WHERE Branch IN ('CS', 'ECE')
AND MarksObtained > 800;

SELECT * FROM cs_ece_top_students_temp_vw;

INSERT INTO students VALUES
(23, 'Ritika', 'Malik', 'CS', 'ritika.malik@example.com', NULL, 890);


CREATE OR REPLACE MATERIALIZED VIEW cs_ece_top_students_mat_vw AS
SELECT
  StudentID, FirstName, LastName, Branch, MarksObtained
FROM students
WHERE Branch IN ('CS', 'ECE')
AND MarksObtained > 800;

SELECT * FROM cs_ece_top_students_mat_vw;

INSERT INTO students VALUES
(24, 'Neha', 'Agarwal', 'CS', 'neha.agarwal@example.com', NULL, 920);

REFRESH MATERIALIZED VIEW cs_ece_top_students_mat_vw;

SELECT * FROM students;

SELECT current_user();


---------------------------------------------------------

-- Lecture: Applying and Discovering Tags, Data Lineage in Unity Catalog
-- Purpose: Demonstrate Standard, Temporary, Materialized, Dynamic Views



SET TAG ON SCHEMA retail schema_content = sales_dataset;
UNSET TAG ON SCHEMA retail schema_content;

SET TAG ON SCHEMA retail ready_for_production = Yes;

SET TAG ON COLUMN retail.customers.Email PII = Yes;
UNSET TAG ON COLUMN retail.customers.Email PII;

SELECT * FROM workspace.information_schema.column_tags;
SELECT * FROM workspace.information_schema.table_tags
WHERE tag_name = 'gold_table';

SELECT * FROM workspace.information_schema.schema_tags;

ALTER TABLE customers
SET TAGS ('data_domain' = 'customer_dataset');

ALTER TABLE customers
ALTER COLUMN Region
SET TAGS ('region_domain' = 'North_dataset');


-----------------

USE CATALOG workspace;
USE SCHEMA retail;

CREATE OR REPLACE VIEW enriched_sales AS
SELECT
    -- Sales Table
    s.OrderID,
    s.OrderDate,
    s.Quantity,
    s.Price AS SalesPrice,
    s.Discount,
    s.Profit,
    s.ShipDate,

    -- Customer Table
    c.ID AS CustomerID,
    c.`First Name`,
    c.`Last Name`,
    c.Email,
    c.City,
    c.State,
    c.Country,
    c.Region,

    -- Product Table
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.unit_price_mrp,

    -- Shipping Table
    sh.ShippingID,
    sh.Ship_Mode,
    sh.Shipping_Cost,
    sh.Carrier

FROM sales s
JOIN customers c
    ON s.CustomerID = c.ID
JOIN products p
    ON s.ProductID = p.product_id
JOIN shipping sh
    ON s.ShippingID = sh.ShippingID;


CREATE OR REPLACE VIEW sales_profit_analysis AS
SELECT
    OrderID,
    OrderDate,
    Region,
    category,
    subcategory,
    Quantity,
    SalesPrice,
    Discount,
    Profit,
    Shipping_Cost,
    (SalesPrice * Quantity) AS total_sales,
    (SalesPrice * Quantity) - Shipping_Cost AS net_revenue
FROM enriched_sales;


CREATE OR REPLACE VIEW category_sales_summary AS
SELECT
    category,
    subcategory,
    Region,
    SUM(total_sales) AS total_sales,
    SUM(Profit) AS total_profit
FROM sales_profit_analysis
GROUP BY category, subcategory, Region;


CREATE OR REPLACE VIEW regional_sales_summary AS
SELECT
    Region,
    SUM(total_sales) AS region_sales,
    SUM(total_profit) AS region_profit
FROM category_sales_summary
GROUP BY Region;


