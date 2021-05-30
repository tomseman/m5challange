
--walmarkt is an international retailer--> keep your data international
--talk about corporate name conventions

--check if schema exits (with python)


SELECT EXISTS(SELECT * FROM information_schema.schemata 
WHERE schema_name = 'walmart');

CREATE SCHEMA IF NOT EXISTS walmart;

--check if table exits (with python)
select exists(select * from pg_catalog.pg_tables
where tablename = 't_item' 
and schemaname = 'walmart');

DROP TABLE walmart.t_item;
CREATE TABLE walmart.t_item(
item_id varchar(200),
department_id varchar(200),
category varchar(200),
valid_from date NOT NULL DEFAULT '1900-01-01',
valid_to date NOT NULL DEFAULT '9999-12-31',
PRIMARY KEY (item_id, valid_to)
);


CREATE TABLE walmart.t_store(
id varchar(10) NOT NULL,
country char(2) NOT NULL,
valid_from date NOT NULL DEFAULT '1900-01-01',
valid_to date NOT NULL DEFAULT '9999-12-31',
PRIMARY KEY (id, valid_to)
);

DROP TABLE walmart.t_sale;
CREATE TABLE walmart.t_sale(
"date" date NOT NULL,
store_id varchar(10) NOT NULL,
item_id varchar(50) NOT NULL,
cnt int NOT NULL,
price_per_unit int NULL DEFAULT 0.0,
PRIMARY KEY ("date",store_id,item_id)
);

--be aware of first day of week
CREATE TABLE walmart.t_calendar(
"date" date NOT NULL,
"year" int NOT NULL,
"month" int NOT NULL,
"day_of_month" int NOT NULL,
"week" int NOT NULL,
"day_of_year" int NOT NULL,
--'day_of_week' int NOT NULL,
PRIMARY KEY ("date")
);


DROP TABLE walmart.t_event_long ;
CREATE TABLE walmart.t_event_long(
"date" date NOT NULL,
"name" varchar(100),
category varchar(100),
country char(2),
working_day BOOLEAN DEFAULT TRUE,
PRIMARY KEY ("date","name","country")
);

CREATE TABLE walmart.t_event_category(
id int NOT NULL,
"name" varchar(100) NOT NULL,
valid_from date NOT NULL DEFAULT '1900-12-31',
valid_to date NOT NULL DEFAULT '9999-12-31',
PRIMARY KEY (id, valid_to)
);


DROP TABLE walmart.t_country;
CREATE TABLE walmart.t_country(
name_short char(2) NOT NULL, --ISO: 3166
name_long varchar(100) NOT NULL,
first_day_of_week_en varchar(100), --FIRST DAY OF week IN english, eg: USA: Sunday 
valid_from date DEFAULT '1900-01-01',
valid_to date DEFAULT '9999-12-31',
PRIMARY KEY (name_short, valid_to)
);

CREATE VIEW walmart.v_event AS
BEGIN
		
END 



--insert test events
INSERT INTO walmart.t_event_long



--########################################
-- Old stuff:


create table adtask.t_holiday(
[date] date not null,
country char(2) null,
event_name varchar(100) null,	
event_type int null,
)


create table adtask.t_event_type(
type_id int not null,
is_gov int not null,
is_religious int not null,
country 
)



create table adtask.t_country_code(
country_iso2 char(2) not null, --iso 2 country code 
country_long varchar(80) not null, --longest offical name: The United Kingdom of Great Britain and Northern Ireland: 56 chars
valid_from date not null,
valid_to date not null
)



copy t_sales_train_tmp 
from '~/projects/m5challange/data/sales_train_valuation.csv'
delimiter ',' 
csv header;










