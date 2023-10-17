-- Databases, schema, external stage, file format --

create database load;
create schema load.markazapp;

create database stage;
create schema stage.markazapp;

create database analytics;
create schema analytics.markazapp;


CREATE OR REPLACE STAGE load.markazapp.s3_stage URL = 's3://markazapp-data/markazapp-raw/'
  CREDENTIALS=(AWS_KEY_ID='AKIATWFTPPO7AQAROYNX' AWS_SECRET_KEY='');

CREATE OR REPLACE FILE FORMAT load.markazapp.markazapp_fileformat type = 'csv' field_delimiter = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' skip_header = 1;  



-- LOAD Database tables --

-- Transactions --

create or replace table load.markazapp.transactions
(
    customer_id varchar,
    product_id varchar,
    purchase_date varchar,
    quantity varchar,
    product_price varchar,
    total_price varchar,
    rating varchar,
    review varchar
);

-- STAGE Database tables --

-- Transactions --

create or replace table stage.markazapp.transactions
(
    customer_id int,
    product_id int,
    purchase_date date,
    quantity int,
    product_price decimal(38,2),
    total_price decimal(38,2),
    rating decimal(38,2),
    review varchar,
    created_at timestamp,
    updated_at timestamp
);