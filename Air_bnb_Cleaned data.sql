-- Get an overview of the raw data********************
select*
from airbnb_open_data;

-- Step 1: Create a staging table for data manipulation (preserving the original data)*********
Create table airbnb_staging1
like airbnb_open_data;

-- Step 2: Insert data from the original table into the staging table************
Insert airbnb_staging1
select *
from airbnb_open_data;

-- Step 4: Change column names and data types for consistency and better data handling***********
Alter table airbnb_staging1
Change `neighbourhood group` neighbourhood_group text,
change `host id` host_id text,
Change `country code` country_code text,
Change `room type` room_type text,
Change `construction year` construction_year int,
Change `service fee in` service_fee text,
Change `Minimum nights` minimum_nights int,
Change `number of reviews` number_of_reviews int,
Change `last review` last_review Text,
Change `Reviews per month` reviews_per_month double,
Change `review rate number` review_rate_number int,
Change `calculated host listings count` host_listings_count int,
Change `availability 365` availability_365 int,
change `long` longitude double;

-- Step 5: Create a new table with distinct values, avoiding duplicates***************
Create Table Airbnb_staging2
Select distinct id, host_id, host_identity_verified, neighbourhood_group, 
neighbourhood, lat, longitude, country, country_code, instant_bookable, cancellation_policy, 
room_type, construction_year, price, service_fee, minimum_nights, 
last_review, reviews_per_month, host_listings_count, availability_365
From airbnb_staging1;

-- Step 6: Trim spaces from all text columns for clean data**************
UPDATE airbnb_staging2
SET
    id = TRIM(id),
    host_id = TRIM(host_id),
    host_identity_verified = TRIM(host_identity_verified),
    neighbourhood_group = TRIM(neighbourhood_group),
    neighbourhood = TRIM(neighbourhood),
    lat = TRIM(lat),
    longitude = TRIM(longitude),
    country = TRIM(country),
    country_code = TRIM(country_code),
    instant_bookable = TRIM(instant_bookable),
    cancellation_policy = TRIM(cancellation_policy),
    room_type = TRIM(room_type),
    construction_year = TRIM(construction_year),
    price = TRIM(price),
    service_fee = TRIM(service_fee),
    minimum_nights = TRIM(minimum_nights),
    last_review = TRIM(last_review),
    reviews_per_month = TRIM(reviews_per_month),
    host_listings_count = TRIM(host_listings_count),
    availability_365 = TRIM(availability_365);

-- Step 7: Remove rows where price is missing****************
Delete
from airbnb_staging2
where price is Null;

-- Step 8: Handle missing values and set defaults where applicable***************
UPDATE airbnb_staging2
SET 
    country = CASE 
        WHEN country is NULL THEN 'United States' ELSE country 
    END,
    country_code = CASE 
        WHEN country_code is NULL THEN 'US' ELSE country_code 
    END,
    neighbourhood_group = CASE 
        WHEN neighbourhood_group is NULL AND neighbourhood = 'williamsburg' THEN 'Brooklyn' ELSE neighbourhood_group 
    END WHERE country is NULL
   OR country_code is NULL
   OR (neighbourhood_group IS NULL AND neighbourhood = 'williamsburg')
   ;   


/* (Additional step in data cleaning) To convert the instant_bookable and host_identity_verified columns with boolean values stored as text, 
into Enum data type in order to still represent 'true' or 'false' as 
strings rather than boolean values of 1 and 0************************************************
*/
ALTER TABLE airbnb_staging2
ADD COLUMN instant_bookable_bool ENUM('true', 'false');

UPDATE airbnb_staging2
SET instant_bookable_bool = CASE
    WHEN instant_bookable = 'true' THEN 'true'
    WHEN instant_bookable = 'false' THEN 'false'
    ELSE NULL
END;

ALTER TABLE airbnb_staging2
DROP COLUMN instant_bookable;

ALTER TABLE airbnb_staging2
CHANGE COLUMN instant_bookable_bool instant_bookable ENUM('true', 'false');

-- Step 9: Create a new column for cleaning the last_review date**************
ALTER TABLE airbnb_staging2 
ADD COLUMN last_review_clean DATE;

/* Step 10: Clean and convert last_review from various text formats to DATE type; 
This was done because the dates had various text formats, so i had to broadenthe qeury to
account for any text format while converting to date data type*************************************
*/
UPDATE airbnb_staging2
SET last_review_clean = CASE

    WHEN last_review REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN STR_TO_DATE(last_review, '%m/%d/%Y')
    WHEN last_review REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(last_review, '%d/%m/%Y')
    WHEN last_review REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN STR_TO_DATE(last_review, '%Y/%m/%d')
    WHEN last_review REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(last_review, '%Y-%m-%d')
    WHEN last_review REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(last_review, '%d-%m-%Y')
    WHEN last_review REGEXP '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$' THEN STR_TO_DATE(last_review, '%m-%d-%Y')
    ELSE NULL 
END WHERE last_review IS NOT NULL AND last_review <> '';

-- Step 11: Drop the old last_review column and rename the cleaned column*******************
ALTER TABLE airbnb_staging2 
DROP COLUMN last_review;

ALTER TABLE airbnb_staging2 
CHANGE last_review_clean last_review DATE;

-- Step 12: Fix specific typos in neighbourhood_group data**********************
UPDATE airbnb_staging2
SET neighbourhood_group = CASE
    WHEN neighbourhood_group = 'brookln' THEN 'Brooklyn'
    WHEN neighbourhood_group = 'manhatan' THEN 'Manhattan'
    ELSE neighbourhood_group
END WHERE neighbourhood_group IN ('brookln', 'manhatan');

-- Step 13: Add new columns for cleaned price and service_fee, as numeric values****************
ALTER TABLE airbnb_staging2
ADD COLUMN price_in_$ INT,
ADD COLUMN Service_fee_$ INT;

-- Step 14: Clean and convert price and service_fee columns to numeric values*****************
UPDATE airbnb_staging2
SET 
price_in_$ = CASE
    WHEN TRIM(price) = '' THEN NULL
    ELSE CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS UNSIGNED)
END,
Service_fee_$ = Case
	When TRIM(service_fee) = '' Then null
    Else Cast(Replace(Replace(service_fee, '$', ''),',', '') As Unsigned)
    End ;

ALTER TABLE airbnb_staging2
MODIFY COLUMN price_in_$ DECIMAL(10, 2),
MODIFY COLUMN Service_fee_$ DECIMAL(10, 2);
    
-- Step 15: Remove the old price and service_fee columns**********************
Alter table airbnb_staging2
drop Column Service_fee,
drop column price;
