CREATE DATABASE hr_dep;

-- DATA CLEANING & TRANSFORMATION

-- OBJECTIVES
-- 1. Standardize all date columns: birthdate, hire_date, and termdate.
-- 2. Correct erroneous birthdates that exceed the current date.

-- OVERVIEW
-- The initial data exploration reveals inconsistencies in the date columns, where different 
-- date separators ('/' and '-') are used. To standardize these dates into a uniform format, we first 
-- need to determine all possible separators present in the dataset.

-- To achieve this, we employ a recursive query to break down each date into its individual characters. 
-- By isolating and analyzing these characters, we apply a DISTINCT function to identify all unique 
-- separators. This approach allows us to confirm that the dataset contains only two types of separators: 
-- '/' and '-'. 

-- With this knowledge, we proceed with transforming the dates into a consistent format, ensuring 
-- accuracy and improving data quality for further analysis. We apply this process to each date column.

WITH RECURSIVE total AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1
    FROM total
    WHERE n <= (SELECT MAX(LENGTH(birthdate)) FROM employees)
)

SELECT 
	DISTINCT SUBSTRING(birthdate, n, 1) AS characters
FROM employees
JOIN total
    ON total.n <= LENGTH(birthdate)
ORDER BY characters;

-- Now that we know the separator types, we proceed to determine if the order of the date elements is consistent.
-- The dataset contains different date formats, with the most commonly used being: yyyy-mm-dd, mm-dd-yyyy, and dd-mm-yyyy.
-- Before converting these values into proper dates, we need to verify whether the order of the date elements aligns 
-- with any of these standard formats.

-- We use the SUBSTRING_INDEX() function along with conditional expressions using CASE WHEN. This combination allows us 
-- to extract all date elements while accounting for the two identified separator types: "-" and "/".

WITH date_components AS (
    SELECT DISTINCT 
        CASE
            WHEN birthdate LIKE '%/%' THEN SUBSTRING_INDEX(birthdate, '/', 1)
            WHEN birthdate LIKE '%-%' THEN SUBSTRING_INDEX(birthdate, '-', 1)
            ELSE birthdate
        END AS element_1,
        CASE
            WHEN birthdate LIKE '%/%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(birthdate, '/', 2), '/', -1)
            WHEN birthdate LIKE '%-%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(birthdate, '-', 2), '-', -1)
            ELSE birthdate
        END AS element_2,
        CASE
            WHEN birthdate LIKE '%/%' THEN SUBSTRING_INDEX(birthdate, '/', -1)
            WHEN birthdate LIKE '%-%' THEN SUBSTRING_INDEX(birthdate, '-', -1)
            ELSE birthdate
        END AS element_3
    FROM employees
)

SELECT
    MAX(element_1) AS element_1,
    MAX(element_2) AS element_2,
    MAX(element_3) AS element_3
FROM date_components;

-- We observe that the dates follow the "mm-dd-yyyy" standard format.

-- Now that we have identified the separator types and the order of the date elements, 
-- we proceed to update the dates into a proper format and convert the column types to DATE.

-- Before executing any DML operations, we start a transaction to ensure data integrity. 
-- This guarantees that in case of an error, we can roll back the database to its previous state. 

-- We apply these updates to both the birthdate and hiredate columns.

START TRANSACTION;

UPDATE employees
SET birthdate = 
    CASE 
        WHEN birthdate LIKE '%/%' THEN STR_TO_DATE(birthdate, '%m/%d/%Y')
        WHEN birthdate LIKE '%-%' THEN STR_TO_DATE(birthdate, '%m-%d-%Y')
        ELSE NULL
    END;

ALTER TABLE employees
MODIFY COLUMN birthdate DATE;

COMMIT;

START TRANSACTION;

UPDATE employees
SET hire_date = 
    CASE
        WHEN hire_date LIKE '%-%' THEN STR_TO_DATE(hire_date, '%m-%d-%Y')
        WHEN hire_date LIKE '%/%' THEN STR_TO_DATE(hire_date, '%m/%d/%Y')
        ELSE NULL
    END;

ALTER TABLE employees
MODIFY COLUMN hire_date DATE;

COMMIT;

-- The termdate column follows a unique format that includes timestamps and timezone information.
-- Since this column only contains dates for employees who have left the company, we standardize it 
-- by converting all blank values to NULL. 

-- Next, we extract only the date portion, removing the time and timezone details. 
-- Finally, we modify the column type to DATE to ensure consistency in storage and further analysis.

START TRANSACTION;

UPDATE employees
SET termdate = NULL
WHERE termdate = '';

UPDATE employees
SET termdate = DATE(STR_TO_DATE(termdate, '%Y-%m-%d %H:%i:%s UTC'))
WHERE termdate IS NOT NULL;

ALTER TABLE employees
MODIFY termdate DATE;

COMMIT;

-- To enhance the analysis, we add an age column that calculates the employee's age 
-- based on the difference, in years, between their birthdate and the current date.

START TRANSACTION;

ALTER TABLE employees 
ADD COLUMN age INT;

UPDATE employees
SET age = TIMESTAMPDIFF(YEAR, birthdate, CURDATE());

COMMIT;

-- After adding the age column and populating its values, we notice a series of negative values.
-- These errors occur due to incorrectly recorded birthdates that extend beyond the current date. 
-- A total of 967 records are affected.

-- Since we do not have access to company records to verify the correct birthdates, we decide to 
-- replace all dates that are beyond today's date minus 18 years with NULL. 
-- The 18-year threshold is chosen as it represents the minimum working age.

-- This adjustment is crucial when writing queries for data exploration, as it prevents inaccurate 
-- calculations based on incorrect birthdates. 

-- Additionally, we update the age column by setting it to NULL for all records where the birthdate 
-- has been corrected, ensuring consistency in the dataset.

START TRANSACTION;

UPDATE employees
SET birthdate = NULL
WHERE birthdate >= DATE_ADD(CURDATE(), INTERVAL -18 YEAR);

UPDATE employees
SET age = NULL
WHERE birthdate IS NULL;

COMMIT;

START TRANSACTION;

UPDATE employees
SET termdate = "1900-01-01"
WHERE termdate >= (
	SELECT max_hiredate
	FROM (
		SELECT MAX(hire_date) AS max_hiredate
		FROM employees
	) AS subquery
);

COMMIT;

-- With this final update, we conclude the data cleaning and transformation process for the dataset.