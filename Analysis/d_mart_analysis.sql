
------------------------------------------------------------------
-- 1. Find the top 3 performing categories per quarter based on total revenue:
-- helps to plan the inventory and sesonal promotions;
with performing_category as(
SELECT p.category,
SUM(p.selling_price * s.quantity) as total_revenue,
DATEPART(QUARTER, s.sale_date) AS quarters,
YEAR(s.sale_date) AS years
FROM products as p
join sales s
on p.product_id = s.product_id
group by p.category,DATEPART(QUARTER, s.sale_date),YEAR(s.sale_date)
),
ranking as (
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years, quarters ORDER BY total_revenue desc) as rnk
FROM performing_category)

select category, total_revenue, years, quarters
from ranking
where rnk <=3;

-- # 2 show revenue share by category per month, including percentage of total
-- Identify monthly category performace
with revenue_cte as (
SELECT MONTH(s.sale_date) AS Month_No,
        DATENAME(MONTH, s.sale_date) AS Month_Name,
        p.category AS Category,
        SUM(s.quantity * p.selling_price) AS Revenue
FROM sales as s
join products as p
on s.product_id = p.product_id
group by 
month(s.sale_date),datename(Month, s.sale_date),p.category
),
final_cte as (
select Month_No, Month_Name, Category, Revenue,
sum(Revenue) over(PARTITION BY Month_No) as total_monthly_revenue
from revenue_cte
)
select Month_Name,
    Category,
    Revenue,
    ROUND((Revenue * 100/ total_monthly_revenue),2) as pct_contrinution,
    DENSE_RANK() over(partition by Month_Name order by Revenue) as Monthly_rank
from final_cte
order by Month_Name, Monthly_rank, pct_contrinution, Category desc;


-- Top 10 customer by lifetime value: - For loyalty and reward program
with life_time_val as (
SELECT c.customer_name, c.customer_id,
sum(s.total_amount) as Total_Life_time_value
FROM sales s
join customers as c
on s.customer_id = c.customer_id
group by c.customer_name, c.customer_id)
, ranking as 
(select *,
dense_rank() over(order by Total_Life_time_value desc) as rnk
from life_time_val
)
select customer_name, customer_id, Total_Life_time_value
from ranking
where rnk <=10;

-- 4. Find out the Highest Revenue month by category;
-- Improve supply chain and ad planning:
WITH MonthlyCategoryRevenue AS (
    SELECT 
        MONTH(s.sale_date) AS Month_No,
        DATENAME(MONTH, s.sale_date) AS Month_Name,
        p.category AS Category,
        SUM(p.selling_price * s.quantity) AS Total_Revenue
    FROM dbo.products AS p
    INNER JOIN dbo.sales AS s
        ON p.product_id = s.product_id
    GROUP BY 
        MONTH(s.sale_date),
        DATENAME(MONTH, s.sale_date),
        p.category
),
TopCategoryByMonth AS (
    SELECT 
        Month_No,
        Month_Name,
        Category,
        Total_Revenue,
        MAX(Total_Revenue) OVER (PARTITION BY Month_No) AS Highest_Revenue
    FROM MonthlyCategoryRevenue
)
SELECT 
    Month_Name,
    Category,
    Highest_Revenue AS Top_Revenue
FROM TopCategoryByMonth
WHERE Total_Revenue = Highest_Revenue
ORDER BY 
    Month_No;


--5. Compare average order value (AOV) between new vs repeat customers for each month. 
-- shows customer acquisiton bs retentioin quality
WITH revenuedata AS (
SELECT 
--MONTH(s.sale_date) AS month_no,
DATENAME(MONTH, s.sale_date) AS month_name,
c.customer_id,
SUM(p.selling_price * s.quantity) AS total_revenue,
COUNT(*) AS order_count,
CASE 
WHEN MONTH(s.sale_date) = MONTH(c.join_date) THEN 'New'
ELSE 'Repeat'
END AS customer_type
FROM sales AS s
JOIN products AS p
ON s.product_id = p.product_id
JOIN customers AS c
ON s.customer_id = c.customer_id
GROUP BY  
--MONTH(s.sale_date),
DATENAME(MONTH, s.sale_date),
c.customer_id,
CASE 
WHEN MONTH(s.sale_date) = MONTH(c.join_date) THEN 'New'
ELSE 'Repeat'
END
), 
aov_by_type as (
SELECT --month_no,
month_name, customer_type,
sum(total_revenue) * 1.0/sum(order_count) as aov
FROM revenuedata
group by --month_no,
month_name, customer_type)
select t1.month_name,
t1.aov as aov_new,
t2.aov as aov_repeat,
round(((t2.aov - t1.aov)/ t2.aov) * 100,2) as diff_in_pct
from aov_by_type as t1
join aov_by_type as t2
on t1.month_name = t2.month_name
and t1.customer_type = 'New'
and t2.customer_type = 'Repeat'
order by t1.month_name;

-- # 6 Show month-over-month growth % in revenue for each product category.
--MoM growth tells whether your business is growing or shrinking — and why.
--It helps you act early, plan better, and improve performance category-wise.
with revenuedata as (
select p.category,
month(s.sale_date) as month_no,
sum(p.selling_price * s.quantity) as total_rev
--lag(sum(p.selling_price * s.quantity)) over(partition by p.category order by month(s.sale_date)) as prev_month_rev
from sales as s
join products as p
on s.product_id =  p.product_id
group by  p.category,month(s.sale_date)
),
final_cte as (
select *, 
lag(total_rev) over(partition by category order by month_no) as prev_month_rev
from revenuedata)
select category, month_no, total_rev, prev_month_rev,
case when prev_month_rev is null then NULL
else round(((total_rev - prev_month_rev) * 100.0/prev_month_rev),2)
end as mom_growth_pct
from final_cte

-- # 7 Identify customers who increased their spending for 3 consecutive months.
-- get the high engagement customers
with MonthlySpending as (
select customer_id,MONTH(sale_date) as month_no,
sum(total_amount) as monthly_spend
from sales
group by customer_id, month(sale_date)
),
PrevSpending as (
select *, lag(monthly_spend) over(partition by customer_id order by month_no) as 
prev_month_1,
 lag(monthly_spend,2) over(partition by customer_id order by month_no) as 
prev_month_2
from MonthlySpending
)
select customer_id, month_no, monthly_spend
from PrevSpending
where prev_month_2 is not null
and prev_month_2 < prev_month_1 and prev_month_1< monthly_spend
order by month_no, customer_id;

-- # 8.Find profit margin variance across cities and rank them.
WITH cte1 AS (
SELECT 
sl.store_id,
st.location,
st.store_name,
CAST(
    ( SUM((pr.selling_price - pr.cost_price) * sl.quantity)
      / NULLIF(SUM(pr.cost_price * sl.quantity), 0) * 100
    ) AS DECIMAL(10,2)
) AS Profit_Margin
FROM sales sl
    JOIN products pr ON sl.product_id = pr.product_id
    JOIN stores st ON st.store_id = sl.store_id
    GROUP BY sl.store_id, st.location, st.store_name
)
SELECT *,
RANK() OVER (ORDER BY Profit_Margin DESC) AS Ranking
FROM cte1
ORDER BY Ranking;


-- 9.Detect outlier customers whose total spend > 2× average spend for that month.
with MonthlySpend as(
select c.customer_name, year(s.sale_date) as year_,
month(s.sale_date) as month_,
sum(s.total_amount) as spend
from sales s
join customers c
on s.customer_id = c.customer_id
group by c.customer_name, year(s.sale_date),
month(s.sale_date)
),
AvgMonthlySpending as (
select *,avg(spend) over(partition by year_,month_) as avg_monthly_sales
from MonthlySpend)
select *, (spend - (2*avg_monthly_sales)) as excess_amt
from AvgMonthlySpending
where spend > 2*avg_monthly_sales
order by spend desc;

-- # 10 Find products with 3-month continuous decline in sales quantity.
with MonthlyQuantity as (
select  p.product_id, p.product_name,MONTH(s.sale_date) as month_,
datename(MONTH, s.sale_date) as month_name,
sum(s.quantity) as total_quantity
from sales s
join products p on
s.product_id = p.product_id
group by  p.product_id,p.product_name,MONTH(s.sale_date),datename(MONTH, s.sale_date)
), PrevQty as (
select *,
lag(total_quantity) over(partition by product_id order by month_) Prev_Month_1,
LAG(total_quantity, 2) OVER (PARTITION BY product_id ORDER BY month_) AS Prev_Month_2
from MonthlyQuantity)
select * from PrevQty
where Prev_Month_2 is not null
and total_quantity < Prev_Month_1
and Prev_Month_1 < Prev_Month_2
order by product_id, month_

-- #11 Products with 3-month declining trend in sales. 

with cte1 as 
(select 
	p.product_id, p.product_name,
    month(s.sale_date) as Month_,
    sum(s.total_amount) as curr_Month_Sales 
from sales s
join products p
on s.product_id = p.product_id
GROUP BY month(s.sale_date) , Product_name ,p.product_id),

cte2 as(
	SELECT * , 
			lag(curr_Month_Sales  , 1) over(PARTITION BY product_id order by Month_) 
				as Last_Month_Sales_1,
            lag(curr_Month_Sales ,2) OVER(PARTITION BY product_id ORDER BY month_) 
				as Last_Month_Sales_2
	from cte1
	)
    
select product_id,product_name,Month_,
Last_Month_Sales_2,Last_Month_Sales_1,curr_Month_Sales 
from cte2
where curr_Month_Sales  < Last_Month_Sales_1 
and Last_Month_Sales_1 < Last_Month_Sales_2
order by  Product_id,Month_;