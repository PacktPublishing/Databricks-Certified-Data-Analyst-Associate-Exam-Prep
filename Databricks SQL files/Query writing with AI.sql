-- Databricks notebook source
-- Count products by subcategory and order by product count descending
USE CATALOG workspace; -- Set the catalog to workspace
USE SCHEMA retail;    -- Set the schema to retail

SELECT 
    subcategory, -- Select the subcategory column
    COUNT(product_id) AS product_count -- Count the number of products in each subcategory
FROM products -- From the products table
GROUP BY subcategory -- Group by subcategory
ORDER BY product_count DESC; -- Order the results by product count in descending order

-- COMMAND ----------

-- Show available catalogs, schemas, and tables

SHOW CATALOGS; -- List all available catalogs

SHOW SCHEMAS; -- List all schemas in the current catalog

SHOW TABLES; -- List all tables in the current schema

-- COMMAND ----------

-- Count students by branch and order by student count descending
SELECT 
    Branch, -- Select the branch column
    COUNT(StudentID) AS student_count -- Count the number of students in each branch
FROM 
    workspace.default.students -- From the students table in workspace.default schema
GROUP BY 
    Branch -- Group by branch
ORDER BY 
    student_count DESC; -- Order by student count descending

-- COMMAND ----------

-- Calculate monthly revenue by year and month
SELECT
    YEAR(OrderDate) AS order_year, -- Extract year from OrderDate
    MONTH(OrderDate) AS order_month, -- Extract month from OrderDate
    SUM(Price * Quantity) AS monthly_revenue -- Calculate total revenue for each month
FROM
    sales -- From the sales table
GROUP BY
    order_year, -- Group by year
    order_month -- Group by month
ORDER BY
    order_year, -- Order by year
    order_month; -- Order by month

-- COMMAND ----------

-- Show top 10 customers by total profit
SELECT
    CONCAT(c.`First Name`, ' ', c.`Last Name`) AS customer_name, -- Combine first and last name for customer name
    SUM(s.Profit) AS total_profit -- Sum profit for each customer
FROM
    sales s -- From sales table (aliased as s)
JOIN
    customers c -- Join with customers table (aliased as c)
ON
    s.CustomerID = c.ID -- Match sales to customers by CustomerID
GROUP BY
    customer_name -- Group by customer name
ORDER BY
    total_profit DESC -- Order by total profit descending
LIMIT 10; -- Show only top 10 customers

-- COMMAND ----------

-- Count new customers in 2022 who did not order before 2022
SELECT 
    COUNT(DISTINCT CustomerID) AS new_customers_2022 -- Count unique customers who ordered in 2022
FROM 
    sales -- From sales table
WHERE 
    YEAR(OrderDate) = 2022 -- Only consider orders in 2022
    AND CustomerID NOT IN ( -- Exclude customers who ordered before 2022
        SELECT DISTINCT CustomerID
        FROM sales
        WHERE OrderDate < '2022-01-01' -- Filter for orders before 2022
    ); -- End of subquery

-- COMMAND ----------

-- Classify customers based on their total sales and show sales by customer
SELECT
    c.ID AS CustomerID, -- Select customer ID
    CONCAT(c.`First Name`, ' ', c.`Last Name`) AS customer_name, -- Combine first and last name
    SUM(s.Price * s.Quantity) AS total_sales, -- Calculate total sales for each customer
    CASE -- Classify customers based on total sales
        WHEN SUM(s.Price * s.Quantity) >= 15000 THEN 'Platinum' -- Classify as Platinum for sales >= 15000
        WHEN SUM(s.Price * s.Quantity) >= 10000 THEN 'Gold' -- Classify as Gold for sales >= 10000
        WHEN SUM(s.Price * s.Quantity) >= 5000 THEN 'Silver' -- Classify as Silver for sales >= 5000
        ELSE 'Bronze' -- Classify as Bronze for sales < 5000
    END AS customer_class
FROM
    sales s -- From sales table (aliased as s)
JOIN
    customers c -- Join with customers table (aliased as c)
ON
    s.CustomerID = c.ID -- Match sales to customers by CustomerID
GROUP BY
    c.ID,
    customer_name -- Group by customer ID and name
ORDER BY
    total_sales DESC; -- Order by total sales descending

-- COMMAND ----------

-- Get the latest order for each customer using a CTE
WITH latest_orders AS (
    SELECT
        OrderID, -- Select order ID
        OrderDate, -- Select order date
        Quantity, -- Select quantity
        Price, -- Select price
        Discount, -- Select discount
        Profit, -- Select profit
        CustomerID, -- Select customer ID
        ProductID, -- Select product ID
        ShippingID, -- Select shipping ID
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC) AS rn -- Assign row number, 1 is latest order per customer
    FROM
        sales -- From sales table
)
SELECT * -- Select all columns
FROM latest_orders -- From the CTE
WHERE rn = 1; -- Only keep the latest order per customer

-- COMMAND ----------

-- DBTITLE 1,Cell 1: Count products by subcategory (PySpark)
-- MAGIC %python
-- MAGIC # Count products by subcategory and order by product count descending
-- MAGIC products_df = spark.read.table("workspace.retail.products")
-- MAGIC
-- MAGIC subcat_counts = (
-- MAGIC     products_df.groupby("subcategory")
-- MAGIC     .agg({"product_id": "count"})
-- MAGIC     .withColumnRenamed("count(product_id)", "product_count")
-- MAGIC     .orderBy("product_count", ascending=False)
-- MAGIC )
-- MAGIC
-- MAGIC display(subcat_counts)

-- COMMAND ----------

-- DBTITLE 1,Cell 2: Show catalogs, schemas, tables (PySpark)
-- MAGIC %python
-- MAGIC # Show available catalogs, schemas, and tables
-- MAGIC catalogs = spark.sql("SHOW CATALOGS")
-- MAGIC schemas = spark.sql("SHOW SCHEMAS")
-- MAGIC tables = spark.sql("SHOW TABLES")
-- MAGIC
-- MAGIC print("Catalogs:")
-- MAGIC display(catalogs)
-- MAGIC print("Schemas:")
-- MAGIC display(schemas)
-- MAGIC print("Tables:")
-- MAGIC display(tables)

-- COMMAND ----------

-- DBTITLE 1,Cell 3: Count students by branch (PySpark)
-- MAGIC %python
-- MAGIC # Count students by branch and order by student count descending
-- MAGIC students_df = spark.read.table("workspace.default.students")
-- MAGIC
-- MAGIC branch_counts = (
-- MAGIC     students_df.groupby("Branch")
-- MAGIC     .agg({"StudentID": "count"})
-- MAGIC     .withColumnRenamed("count(StudentID)", "student_count")
-- MAGIC     .orderBy("student_count", ascending=False)
-- MAGIC )
-- MAGIC
-- MAGIC display(branch_counts)

-- COMMAND ----------

-- DBTITLE 1,Cell 4: Monthly revenue by year and month (PySpark)
-- MAGIC %python
-- MAGIC # Calculate monthly revenue by year and month
-- MAGIC from pyspark.sql.functions import year, month, sum
-- MAGIC
-- MAGIC sales_df = spark.read.table("workspace.retail.sales")
-- MAGIC
-- MAGIC monthly_rev = (
-- MAGIC     sales_df.withColumn("order_year", year("OrderDate"))
-- MAGIC            .withColumn("order_month", month("OrderDate"))
-- MAGIC            .groupBy("order_year", "order_month")
-- MAGIC            .agg(sum("Price" * "Quantity").alias("monthly_revenue"))
-- MAGIC            .orderBy("order_year", "order_month")
-- MAGIC )
-- MAGIC
-- MAGIC display(monthly_rev)

-- COMMAND ----------

-- DBTITLE 1,Cell 5: Top 10 customers by profit (PySpark)
-- MAGIC %python
-- MAGIC # Show top 10 customers by total profit
-- MAGIC from pyspark.sql.functions import concat_ws, sum
-- MAGIC
-- MAGIC sales_df = spark.read.table("workspace.retail.sales")
-- MAGIC customers_df = spark.read.table("workspace.retail.customers")
-- MAGIC
-- MAGIC joined = sales_df.join(customers_df, sales_df.CustomerID == customers_df.ID, "inner")
-- MAGIC joined = joined.withColumn(
-- MAGIC     "customer_name", concat_ws(" ", customers_df["First Name"], customers_df["Last Name"])
-- MAGIC )
-- MAGIC
-- MAGIC # Aggregate, order descending, and select top 10
-- MAGIC
-- MAGIC top_profits = (
-- MAGIC     joined.groupBy("customer_name")
-- MAGIC           .agg(sum("Profit").alias("total_profit"))
-- MAGIC           .orderBy("total_profit", ascending=False)
-- MAGIC           .limit(10)
-- MAGIC )
-- MAGIC
-- MAGIC display(top_profits)

-- COMMAND ----------

-- DBTITLE 1,Cell 6: Count new customers in 2022 (PySpark)
-- MAGIC %python
-- MAGIC # Count new customers in 2022 who did not order before 2022
-- MAGIC from pyspark.sql.functions import year
-- MAGIC
-- MAGIC sales_df = spark.read.table("workspace.retail.sales")
-- MAGIC orders_2022 = sales_df.filter(year("OrderDate") == 2022)
-- MAGIC earlier_orders = sales_df.filter(sales_df.OrderDate < "2022-01-01")
-- MAGIC prior_customers = earlier_orders.select("CustomerID").distinct()
-- MAGIC
-- MAGIC new_customers = orders_2022.join(
-- MAGIC     prior_customers,
-- MAGIC     orders_2022.CustomerID == prior_customers.CustomerID,
-- MAGIC     "left_anti"
-- MAGIC )
-- MAGIC new_customers_count = new_customers.select("CustomerID").distinct().count()
-- MAGIC
-- MAGIC print(f"New customers in 2022: {new_customers_count}")

-- COMMAND ----------

-- DBTITLE 1,Cell 7: Customer classification by sales (PySpark)
-- MAGIC %python
-- MAGIC # Classify customers based on their total sales and show sales by customer
-- MAGIC from pyspark.sql.functions import sum, concat_ws, when
-- MAGIC
-- MAGIC sales_df = spark.read.table("workspace.retail.sales")
-- MAGIC customers_df = spark.read.table("workspace.retail.customers")
-- MAGIC
-- MAGIC df = sales_df.join(customers_df, sales_df.CustomerID == customers_df.ID, "inner")
-- MAGIC df = df.withColumn("customer_name", concat_ws(" ", customers_df["First Name"], customers_df["Last Name"]))
-- MAGIC totals = df.groupBy("ID", "customer_name").agg(sum(df.Price * df.Quantity).alias("total_sales"))
-- MAGIC
-- MAGIC totals = totals.withColumn(
-- MAGIC     "customer_class",
-- MAGIC     when(totals.total_sales >= 15000, "Platinum")
-- MAGIC     .when(totals.total_sales >= 10000, "Gold")
-- MAGIC     .when(totals.total_sales >= 5000, "Silver")
-- MAGIC     .otherwise("Bronze")
-- MAGIC )
-- MAGIC
-- MAGIC totals = totals.orderBy("total_sales", ascending=False)
-- MAGIC display(totals)

-- COMMAND ----------

-- DBTITLE 1,Cell 8: Latest order per customer (PySpark)
-- MAGIC %python
-- MAGIC # Get the latest order for each customer using a CTE-like approach
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import row_number
-- MAGIC
-- MAGIC sales_df = spark.read.table("workspace.retail.sales")
-- MAGIC window_spec = Window.partitionBy("CustomerID").orderBy(sales_df.OrderDate.desc())
-- MAGIC
-- MAGIC latest_orders = sales_df.withColumn("rn", row_number().over(window_spec))
-- MAGIC latest_per_customer = latest_orders.filter(latest_orders.rn == 1)
-- MAGIC display(latest_per_customer)