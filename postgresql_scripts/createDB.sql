CREATE SCHEMA IF NOT EXISTS walmart;

CREATE TABLE IF NOT EXISTS walmart.t_item(
item_id varchar(200),
department_id varchar(200),
category varchar(200),
valid_from date NOT NULL DEFAULT '1900-01-01',
valid_to date NOT NULL DEFAULT '9999-12-31',
PRIMARY KEY (item_id, valid_to)
);

CREATE TABLE IF NOT EXISTS walmart.t_store(
id varchar(10) NOT NULL,
country char(2) NOT NULL,
valid_from date NOT NULL DEFAULT '1900-01-01',
valid_to date NOT NULL DEFAULT '9999-12-31',
PRIMARY KEY (id, valid_to)
);

CREATE TABLE IF NOT EXISTS walmart.t_sale(
"date" date NOT NULL,
store_id varchar(10) NOT NULL,
item_id varchar(50) NOT NULL,
cnt int NOT NULL,
price_per_unit decimal(28,10) NULL DEFAULT 0.0,
PRIMARY KEY ("date",store_id,item_id)
);

CREATE TABLE IF NOT EXISTS walmart.t_calendar(
"date" date NOT NULL,
"year" int NOT NULL,
"month" int NOT NULL,
"day_of_month" int NOT NULL,
"day_of_week_walmart" int NOT NULL,
"day_of_year" int NOT NULL,
"week_of_year" int NOT NULL,
PRIMARY KEY ("date")
);

CREATE TABLE IF NOT EXISTS walmart.t_event_long(
"date" date NOT NULL,
"name" varchar(100),
category varchar(100),
country char(2),
working_day BOOLEAN DEFAULT TRUE,
PRIMARY KEY ("date","name","country")
);

CREATE TABLE IF NOT EXISTS walmart.t_country(
name_short char(2) NOT NULL, 
name_long varchar(100) NOT NULL,
first_day_of_week_en varchar(100), 
valid_from date DEFAULT '1900-01-01',
valid_to date DEFAULT '9999-12-31',
PRIMARY KEY (name_short, valid_to)
);

CREATE OR REPLACE VIEW walmart.v_event 
AS
 SELECT 
 A."date" AS event_date,
 A.country AS event_country,
 split_part(A.event_name_agg, ',', 1) AS event_name_1,
 split_part(A.category_agg, ',', 1) AS event_category_1,
 split_part(A.event_name_agg, ',', 2) AS event_name_2,
 split_part(A.category_agg, ',', 2) AS event_category_2,
 split_part(A.event_name_agg, ',', 3) AS event_name_3,
 split_part(A.category_agg, ',', 3) AS event_category_3 
 FROM ( 
  SELECT 
    "date", 
    country, 
    string_agg("name" ,',') AS event_name_agg,
    string_agg(category,',') AS category_agg,
    string_agg(CAST(working_day AS VARCHAR(10)),',') AS working_day_agg
    from walmart.t_event_long
    group by "date", "country"
    order by "date", "country"
    ) A; 
  
   
CREATE OR REPLACE VIEW walmart.v_sales AS
SELECT tc.*,
ts.item_id,
ti.department_id,
ti.category,
tst.country,
ve.event_name_1,
ve.event_category_1,
ve.event_name_2,
ve.event_category_2,
ve.event_name_3,
ve.event_category_3,
ts.cnt, 
ts.price_per_unit
FROM walmart.t_calendar tc 
INNER JOIN 
	walmart.t_sale ts
ON
		ts."date" = tc."date" 
INNER JOIN 
	walmart.t_store tst 
ON 
		ts.store_id  = tst.id 
	AND tc."date" BETWEEN tst.valid_from AND tst.valid_to 
INNER JOIN 
	walmart.t_item ti 
ON
		ts.item_id = ti.item_id 
	AND tc."date" BETWEEN ti.valid_from AND ti.valid_to
INNER JOIN 
	walmart.t_country tco 
ON
		tco.name_short = tst.country 
	AND tc."date" BETWEEN tco.valid_from AND tco.valid_to 
LEFT JOIN 
	walmart.v_event ve 
ON 
		ve.event_date = tc."date" 
	AND ve.event_country = tst.country;

