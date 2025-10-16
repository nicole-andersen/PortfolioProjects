-- Project - Exploratory Data Analysis

-- Link to data: https://www.kaggle.com/datasets/yuanchunhong/us-supply-chain-risk-analysis-dataset

SELECT * 
FROM supply_raw;

CREATE TABLE supply_staging
LIKE supply_raw;

INSERT supply_staging
SELECT * 
FROM supply_raw;

SELECT *
FROM supply_staging;



-- Verify data cleaning and normalization

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Product_Category, Quantity_Ordered, Shipping_Mode, Order_Value_USD, Disruption_Type, 
Disruption_Severity, Historical_Disruption_Count, Supplier_Reliability_Score) AS row_num
FROM supply_staging;

WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Product_Category, Quantity_Ordered, Shipping_Mode, Order_Value_USD, Disruption_Type, 
Disruption_Severity, Historical_Disruption_Count, Supplier_Reliability_Score) AS row_num
FROM supply_staging
)
SELECT * 
FROM duplicate_CTE
WHERE row_num > 1;

ALTER TABLE supply_staging
MODIFY COLUMN `Order_Date` DATE;

ALTER TABLE supply_staging
MODIFY COLUMN `Dispatch_Date` DATE;

ALTER TABLE supply_staging
MODIFY COLUMN `Delivery_Date` DATE;



-- Explore trends in shipping mode

SELECT *
FROM supply_staging;

SELECT Product_Category, Quantity_Ordered, Order_Date, Dispatch_Date, Delivery_Date, Shipping_Mode, Order_Value_USD, 
Delay_Days, Disruption_Type, Disruption_Severity, Historical_Disruption_Count, Supplier_Reliability_Score
FROM supply_staging
ORDER BY Delay_Days, Disruption_Severity;

SELECT Product_Category, Quantity_Ordered, Order_Date, Dispatch_Date, Delivery_Date, Shipping_Mode, Order_Value_USD, 
Delay_Days, Disruption_Type, Disruption_Severity, Historical_Disruption_Count, Supplier_Reliability_Score
FROM supply_staging
ORDER BY Historical_Disruption_Count DESC;

SELECT Shipping_Mode, AVG(Supplier_Reliability_Score)
FROM supply_staging
GROUP BY Shipping_Mode;

SELECT Shipping_Mode, SUM(Historical_Disruption_Count), COUNT(Quantity_Ordered)
FROM supply_staging
GROUP BY Shipping_Mode;

SELECT Shipping_Mode, SUM(Historical_Disruption_Count), COUNT(Quantity_Ordered)
FROM supply_staging
WHERE Disruption_Severity = 'High'
GROUP BY Shipping_Mode;

SELECT Product_Category, Shipping_Mode, SUM(Delay_Days) AS Total_Delay_Days, SUM(Historical_Disruption_Count) AS Total_Disruptions, 
COUNT(Quantity_Ordered) AS Total_Orders, (SUM(Delay_Days)/COUNT(Product_Category)) AS Avg_Delay_Per_Order
FROM supply_staging
GROUP BY Product_Category, Shipping_Mode
ORDER BY Shipping_Mode, Total_Orders DESC
;



-- Explore industries most impacted by supply chain delays

SELECT Product_Category, Quantity_Ordered, Order_Date, Dispatch_Date, Delivery_Date, Shipping_Mode, Order_Value_USD, 
Delay_Days, Disruption_Type, Disruption_Severity, Historical_Disruption_Count, Supplier_Reliability_Score
FROM supply_staging;

SELECT Product_Category, AVG(Supplier_Reliability_Score)
FROM supply_staging
GROUP BY Product_Category;

SELECT Product_Category, SUM(Historical_Disruption_Count), COUNT(Quantity_Ordered)
FROM supply_staging
GROUP BY Product_Category;

SELECT Product_Category, Shipping_Mode, 'High Severity' AS Label
FROM supply_staging
WHERE Disruption_Severity = 'High';

SELECT Product_Category, SUM(Historical_Disruption_Count), COUNT(Quantity_Ordered)
FROM supply_staging
WHERE Disruption_Severity = 'High'
GROUP BY Product_Category;

SELECT Disruption_Type, SUM(Historical_Disruption_Count), COUNT(Quantity_Ordered)
FROM supply_staging
GROUP BY Disruption_Type;

SELECT Disruption_Type, SUM(Historical_Disruption_Count), COUNT(Quantity_Ordered)
FROM supply_staging
WHERE Product_Category = 'Food'
GROUP BY Disruption_Type;

SELECT Shipping_Mode, COUNT(Shipping_Mode)
FROM supply_staging
WHERE Product_Category = 'Food'
GROUP BY Shipping_Mode;

SELECT Product_Category, (SUM(Delay_Days)/COUNT(Product_Category))
FROM supply_staging
GROUP BY Product_Category;

SELECT Product_Category, Disruption_Type, SUM(Delay_Days) AS Total_Delay_Days, SUM(Historical_Disruption_Count) AS Total_Disruptions, 
COUNT(Quantity_Ordered) AS Total_Orders, (SUM(Delay_Days)/COUNT(Product_Category)) AS Avg_Delay_Per_Order
FROM supply_staging
GROUP BY Product_Category, Disruption_Type
ORDER BY Disruption_Type, SUM(Delay_Days) DESC;

SELECT Product_Category, Disruption_Type, SUM(Delay_Days) AS Total_Delay_Days, SUM(Historical_Disruption_Count) AS Total_Disruptions, 
COUNT(Quantity_Ordered) AS Total_Orders, (SUM(Delay_Days)/COUNT(Product_Category)) AS Avg_Delay_Per_Order
FROM supply_staging
GROUP BY Product_Category, Disruption_Type
ORDER BY Product_Category, SUM(Delay_Days) DESC;

SELECT Product_Category, SUM(Delay_Days)
FROM supply_staging
GROUP BY Product_Category;








