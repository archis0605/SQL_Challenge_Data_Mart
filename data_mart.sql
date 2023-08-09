/* Case Study Questions
The following case study questions require some data cleaning steps 
before we start to unpack Danny’s key business questions in more depth.*/

/*1. Data Cleansing Steps
In a single query, perform the following operations and generate a new 
table in the data_mart schema named clean_weekly_sales:

Convert the week_date to a DATE format

Add a week_number as the second column for each week_date value, 
for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc

Add a month_number with the calendar month for each week_date value as the 3rd column

Add a calendar_year column as the 4th column containing either 2018, 2019 or 2020 values

Add a new column called age_band after the original segment column using the 
following mapping on the number inside the segment value
segment	age_band
1	Young Adults
2	Middle Aged
3 or 4	Retirees

Add a new demographic column using the following mapping for the first letter in the segment values:
segment	demographic
C	Couples
F	Families

Ensure all null string values with an "unknown" string value in the original segment 
column as well as the new age_band and demographic columns

Generate a new avg_transaction column as the sales value divided by transactions 
rounded to 2 decimal places for each record*/
create view clean_weekly_sales as 
	(select str_to_date(week_date,"%d/%c/%y") as weekly_date,
		region, platform, if(segment = "null", "unknown", segment) as segment,
		customer_type, transactions, sales, week(str_to_date(week_date,"%d/%c/%y")) as week_num,
		month(str_to_date(week_date,"%d/%c/%y")) as month_num, year(str_to_date(week_date,"%d/%c/%y")) as year, 
		case when segment like "%1%" then "Young Adults"
			 when segment like "%2%" then "Middle Aged"
			 when segment like "%3%" or segment like "%4%" then "Retirees" 
			 else "unknown" end as age_brand,
		case when segment like "%C%" then "Couples"
			 when segment like "%F%" then "Families"
			 else "unknown" end as demographics,
			 round((sales/transactions),2) as avg_txn
	from weekly_sales);

/* 2. Data Exploration
/*a. What day of the week is used for each week_date value?*/
with day_of_week as (
	select distinct week_date, dayname(week_date) as nameofday
	from clean_weekly_sales)
select distinct nameofday
from day_of_week;

/*b. What range of week numbers are missing from the dataset?*/
with recursive weeks_in_year as (
    select 1 as week_num
    union all
    select week_num + 1 from weeks_in_year where week_num < 52
),
cte as (
	select distinct week(week_date) as week_num, week_date
	from clean_weekly_sales
	order by 1)
select distinct w1.week_num as missing_weeks
from weeks_in_year w1
left join cte w2 using(week_num)
where w2.week_date is null;

/*c. How many total transactions were there for each year in the dataset?*/
select year, concat(round(sum(transactions)/1000000,2)," M") as total_txn
from clean_weekly_sales
group by 1 order by 1;

/*d. What is the total sales for each region for each month?*/
select region, month_num, concat(round(sum(sales)/1000000,2)," M") as total_sales
from clean_weekly_sales
group by 1,2
order by 1;

/*e. What is the total count of transactions for each platform?*/
select platform, concat(round(sum(transactions)/1000000,2), " M") as txn_count
from clean_weekly_sales
group by 1;

/*f. What is the percentage of sales for Retail vs Shopify for each month?*/
with cte1 as (
	select month_num, 
		sum(case when platform = "Retail" then sales else 0 end) as retail_sales,
		sum(case when platform = "Shopify" then sales else 0 end) as shopify_sales
	from clean_weekly_sales
	group by 1 order by 1)
select month_num, concat(round(retail_sales*100/(retail_sales + shopify_sales),2)," %") as retail_sales_prcnt,
	concat(round(shopify_sales*100/(retail_sales + shopify_sales),2)," %") as shopify_sales_prcnt
from cte1;

/*g. What is the percentage of sales by demographic for each year in the dataset?*/
with cte1 as (
	select year, 
		sum(case when demographics = "Families" then sales else 0 end) as fam_sales,
		sum(case when demographics = "Couples" then sales else 0 end) as coup_sales
	from clean_weekly_sales
	group by 1 order by 1)
select year, concat(round(fam_sales*100/(fam_sales + coup_sales),2)," %") as fam_sales_prcnt,
	concat(round(coup_sales*100/(fam_sales + coup_sales),2)," %") as coup_sales_prcnt
from cte1;

/*h. Which age_band and demographic values contribute the most to Retail sales?*/
select age_brand, demographics, round(sum(transactions)/1000000,2) as total_contribution_mln
from clean_weekly_sales
where platform = "Retail" and age_brand <> "unknown"
group by 1,2
order by 3 desc limit 1;

/*i. Can we use the avg_transaction column to find the average transaction size 
for each year for Retail vs Shopify? If not - how would you calculate it instead?*/
select year, 
	round(avg(case when platform = "Retail" then transactions else 0 end)) as retail_avg_txn,
    round(avg(case when platform = "Shopify" then transactions else 0 end)) as shopify_avg_txn
from clean_weekly_sales
group by 1
order by 1;

/*3. Before & After Analysis
This technique is usually used when we inspect an important event and want 
to inspect the impact before and after a certain point in time.

Taking the week_date value of 2020-06-15 as the baseline week where the 
Data Mart sustainable packaging changes came into effect.

We would include all week_date values for 2020-06-15 as the start of the 
period after the change and the previous week_date values would be before

Using this analysis approach - answer the following questions:*/

/* What is the total sales for the 4 weeks before and after 2020-06-15? 
What is the growth or reduction rate in actual values and percentage of sales?*/
with cte as (
	select
		sum(case when week_date >= date_sub('2020-06-15', interval 4 week) and week_date < '2020-06-15' then sales 
		else 0 end) as sales_before,
		sum(case when week_date > '2020-06-15' and week_date <= date_add('2020-06-15', interval 4 week) then sales 
		else 0 end) as sales_after
	from clean_weekly_sales)
select concat(round(sales_before/1000000000,2), " B") as sales_before,
	concat(round(sales_after/1000000000,2), " B") as sales_after,
    concat(round((sales_after-sales_before)*100/sales_before,2)," %") as growth
from cte;
     
/* What about the entire 12 weeks before and after?*/
with cte as (
	select
		sum(case when week_date >= date_sub('2020-06-15', interval 12 week) and week_date < '2020-06-15' then sales 
		else 0 end) as sales_before,
		sum(case when week_date > '2020-06-15' and week_date <= date_add('2020-06-15', interval 12 week) then sales 
		else 0 end) as sales_after
	from clean_weekly_sales)
select concat(round(sales_before/1000000000,2), " B") as sales_before,
	concat(round(sales_after/1000000000,2), " B") as sales_after,
    concat(round((sales_after-sales_before)*100/sales_before,2)," %") as growth
from cte;

/* How do the sale metrics for these 2 periods before and after 
compare with the previous years in 2018 and 2019?*/
with cte1 as (
	select
		round(sum(case when week_date >= date_sub('2020-06-15', interval 12 week) and week_date < '2020-06-15' then sales 
		else 0 end)/1000000000,1) as sales_before_bln,
		round(sum(case when week_date > '2020-06-15' and week_date <= date_add('2020-06-15', interval 12 week) then sales 
		else 0 end)/1000000000,1) as sales_after_bln
	from clean_weekly_sales),
cte2 as (
	select year, round(sum(sales)/1000000000,1) as total_sales_bln
    from clean_weekly_sales
    where year <> 2020
    group by 1 order by 1)
select year, 
	round((total_sales_bln-(select sales_before_bln from cte1))*100/(select sales_before_bln from cte1),1) as sales_growth_before,
    round((total_sales_bln-(select sales_after_bln from cte1))*100/(select sales_after_bln from cte1),1) as sales_growth_after
from cte2
group by 1;
