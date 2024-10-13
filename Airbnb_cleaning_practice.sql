-- Step 1: Selecting all data from the source table

Select *
From airbnb_open_data;

-- Step 2: Creating a new table and converting blank cells to NULL values for better handling

CREATE TABLE airbnb_trial1 AS
SELECT 
    COALESCE(NULLIF(id, ''), NULL) AS id,  -- Removing lower() from ID as case sensitivity may be important
    COALESCE(NULLIF(TRIM(`host id`), ''), NULL) AS host_id,
    COALESCE(NULLIF(LOWER(TRIM(host_identity_verified)), ''), NULL) AS host_identity_verified,
    COALESCE(NULLIF(LOWER(TRIM(`neighbourhood group`)), ''), NULL) AS neighbourhood_group,
    COALESCE(NULLIF(LOWER(TRIM(neighbourhood)), ''), NULL) AS neighbourhood,
    COALESCE(NULLIF(TRIM(lat), ''), NULL) AS lat,  -- Keeping lat and long without lower() as they are numerical
    COALESCE(NULLIF(TRIM(`long`), ''), NULL) AS longitude,
    COALESCE(NULLIF(LOWER(TRIM(country)), ''), NULL) AS country,
    COALESCE(NULLIF(LOWER(TRIM(`country code`)), ''), NULL) AS country_code,
    COALESCE(NULLIF(LOWER(TRIM(instant_bookable)), ''), NULL) AS instant_bookable,
    COALESCE(NULLIF(LOWER(TRIM(cancellation_policy)), ''), NULL) AS cancellation_policy,
    COALESCE(NULLIF(LOWER(TRIM(`room type`)), ''), NULL) AS room_type,
    COALESCE(NULLIF(TRIM(`construction year`), ''), NULL) AS construction_year, -- No need for LOWER for numerical fields
    COALESCE(NULLIF(TRIM(price), ''), NULL) AS price,
    COALESCE(NULLIF(TRIM(`service fee in`), ''), NULL) AS service_fee,
    COALESCE(NULLIF(TRIM(`minimum nights`), ''), NULL) AS minimum_nights,
    COALESCE(NULLIF(TRIM(`number of reviews`), ''), NULL) AS number_of_reviews,
    COALESCE(NULLIF(TRIM(`last review`), ''), NULL) AS last_review,
    COALESCE(NULLIF(TRIM(`reviews per month`), ''), NULL) AS reviews_per_month,
    COALESCE(NULLIF(TRIM(`review rate number`), ''), NULL) AS review_rate_number,
    COALESCE(NULLIF(TRIM(`calculated host listings count`), ''), NULL) AS host_listings_count,
    COALESCE(NULLIF(TRIM(`availability 365`), ''), NULL) AS availability_365
FROM airbnb_open_data;

-- Step 3: Changing column data types where necessary

ALTER TABLE airbnb_trial1
    CHANGE id id INT,
    CHANGE host_id host_id BIGINT,
    CHANGE host_identity_verified host_identity_verified TEXT,
    CHANGE neighbourhood_group neighbourhood_group TEXT,
    CHANGE neighbourhood neighbourhood TEXT,
    CHANGE lat lat DOUBLE,
    CHANGE longitude longitude DOUBLE,
    CHANGE country country TEXT,
    CHANGE instant_bookable instant_bookable TEXT,
    CHANGE country_code country_code TEXT,
    CHANGE cancellation_policy cancellation_policy TEXT,
    CHANGE room_type room_type TEXT,
    CHANGE construction_year construction_year INT,
    CHANGE price price DECIMAL(10,2),
    CHANGE service_fee service_fee DECIMAL(10,2),
    CHANGE minimum_nights minimum_nights INT,
    CHANGE number_of_reviews number_of_reviews INT,
    CHANGE reviews_per_month reviews_per_month DECIMAL(10,2),
    CHANGE review_rate_number review_rate_number DECIMAL(10,2),
    CHANGE host_listings_count host_listings_count INT,
    CHANGE availability_365 availability_365 INT;

/* Step 4: Adding a clean version of the last_review date column and a
   last_review_conversion_status to track failed date conversion.
*/

Alter table airbnb_trial1
Add column last_review_clean date;

ALTER TABLE airbnb_trial1
ADD COLUMN last_review_conversion_status VARCHAR(10);

-- Step 5: Updating the last_review_clean column with standardized date formats

Update airbnb_trial1
Set last_review_clean = case
	WHEN last_review REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN STR_TO_DATE(last_review, '%m/%d/%Y')
    WHEN last_review REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(last_review, '%d/%m/%Y')
    WHEN last_review REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN STR_TO_DATE(last_review, '%Y/%m/%d')
    WHEN last_review REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(last_review, '%Y-%m-%d')
    WHEN last_review REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN STR_TO_DATE(last_review, '%d-%m-%Y')
    WHEN last_review REGEXP '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$' THEN STR_TO_DATE(last_review, '%m-%d-%Y')
	ELSE NULL 
END,
	last_review_conversion_status = CASE
    WHEN last_review REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN 'success'
    WHEN last_review REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN 'success'
    WHEN last_review REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN 'success'
    WHEN last_review REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 'success'
    WHEN last_review REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN 'success'
    WHEN last_review REGEXP '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$' THEN 'success'
    ELSE 'failure'
End WHERE last_review IS NOT NULL AND last_review <> '';

-- Step 6: Check to see if all date conversion successful

SELECT *
FROM airbnb_trial1
WHERE last_review_conversion_status = 'failure';

-- Step 7: Dropping the original last_review column and renaming last_review_clean

ALTER TABLE airbnb_trial1 
	DROP COLUMN last_review,
	DROP COLUMN last_review_conversion_status;

ALTER TABLE airbnb_trial1 
CHANGE last_review_clean last_review DATE;

-- Step 8: Correcting incorrect values in neighbourhood_group, country, and country_code

Update airbnb_trial1
Set neighbourhood_group = case
	When neighbourhood_group = 'brookln' then 'brooklyn' 
	When neighbourhood_group = 'manhatan' then 'manhattan' 
    else neighbourhood_group
end,
Country = case
	When country is null then 'united states' else country
    end,
Country_code = case
	When country_code is null then 'us' else country_code
    end 
	Where neighbourhood_group = 'brookln' or neighbourhood_group = 'manhatan'
	or country is null or country_code is null;

-- Step 9: Using a self-join to fill in missing neighbourhood_group values

Update airbnb_trial1 as st1
Join airbnb_trial1 as st2
	on st1.neighbourhood = st2.neighbourhood
Set st1.neighbourhood_group = st2.neighbourhood_group
Where st1.neighbourhood_group is Null 
    And st2.neighbourhood_group is not null;

-- Step 10: Checking how many rows still have NULL neighbourhood_group values

SELECT COUNT(*) 
FROM airbnb_trial1 
WHERE neighbourhood_group IS NULL;

-- Step 11: Deleting rows where both price and service_fee are NULL

Delete
From airbnb_trial1
where price is null and service_fee is null;

-- Step 12: Creating the final cleaned dataset with distinct rows

Create Table Airbnb_clean_data
Select distinct *
from airbnb_trial1;
