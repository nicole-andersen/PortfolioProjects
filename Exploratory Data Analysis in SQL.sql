-- Project - Exploratory Data Analysis

-- Link to data: https://www.kaggle.com/datasets/yuanchunhong/us-supply-chain-risk-analysis-dataset

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

-- Explore trends

SELECT Product_Category, Quantity_Ordered, Shipping_Mode, Order_Value_USD, Disruption_Type, 
Disruption_Severity, Historical_Disruption_Count, Supplier_Reliability_Score
FROM supply_staging;

SELECT Product_Category, Quantity_Ordered, Shipping_Mode, Order_Value_USD, Disruption_Type, 
Disruption_Severity, Historical_Disruption_Count, Supplier_Reliability_Score
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

-- The shipping modes are fairly equal in supplier reliability score and historical disruption count, with Sea having slightly higher reliability. 

SELECT Product_Category, Quantity_Ordered, Shipping_Mode, Order_Value_USD, Disruption_Type, 
Disruption_Severity, Historical_Disruption_Count, Supplier_Reliability_Score
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
GROUP BY Shipping_Mode
;

-- Pharma has the highest reliability and the supply chain is most likely to be impacted by weather. Air is the primary form of transport.
-- Food has the lowest reliability and is impacted by strike. 
-- Textiles and electronics are more likely to have "severe" disruptions. Textiles are primarily impacted by customs, which electronics and impacted by shortages.
-- While shortage and weather are common distruptions, customs and strike tend to be more severe. 






