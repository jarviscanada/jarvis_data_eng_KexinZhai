-- Inspect table schema
\d+ retail;

--------------------------------------------------
-- Q1: Show first 10 rows
--------------------------------------------------
SELECT
    invoice_no,
    stock_code,
    description,
    quantity,
    unit_price
FROM retail
ORDER BY invoice_no, stock_code
    LIMIT 10;

--------------------------------------------------
-- Q2: Check # of records
--------------------------------------------------
SELECT COUNT(*) AS count
FROM retail;

--------------------------------------------------
-- Q3: number of clients (e.g. unique client ID)
--------------------------------------------------
SELECT COUNT(DISTINCT customer_id) AS count
FROM retail;

--------------------------------------------------
-- Q4: invoice date range (max / min dates)
--------------------------------------------------
SELECT
    MAX(invoice_date) AS max,
  MIN(invoice_date) AS min
FROM retail;

--------------------------------------------------
-- Q5: number of SKU/merchants (unique stock_code)
--------------------------------------------------
SELECT COUNT(DISTINCT stock_code) AS count
FROM retail;

--------------------------------------------------
-- Q6: average invoice amount excluding
--     invoices with a negative amount
--------------------------------------------------
SELECT AVG(invoice_total) AS avg
FROM (
    SELECT
    invoice_no,
    SUM(unit_price * quantity) AS invoice_total
    FROM retail
    GROUP BY invoice_no
    HAVING SUM(unit_price * quantity) > 0
    ) t;

--------------------------------------------------
-- Q7: total revenue (sum of unit_price * quantity)
--------------------------------------------------
SELECT
    SUM(unit_price * quantity) AS sum
FROM retail;


--------------------------------------------------
-- Q8: total revenue by YYYYMM
--------------------------------------------------
SELECT
    (EXTRACT(YEAR  FROM invoice_date)::int * 100
   + EXTRACT(MONTH FROM invoice_date)::int) AS yyyymm,
    SUM(unit_price * quantity) AS sum
FROM retail
GROUP BY yyyymm
ORDER BY yyyymm;


