-- DATABASE CREATION

-- OBJECTIVES
-- 1. Break the employees dataset into different tables to create a snowflake schema.
-- 2. Establish relationships between the tables.
-- 3. Create a fine-tuned and streamlined database that seamlessly integrates with Power BI.

-- Now we create the core/primary tables. These are the first group of tables that connect to 
-- the fact table.

-- The process for creating the core tables is similar. We create a table based on each column 
-- that we want to store separately and that represent a characteristic of the data in the 
-- fact table.

-- For our database, we create tables for: ethnicity, gender, location, job title, city, 
-- department, and state. Each of these tables consists of a single column containing 
-- DISTINCT values from the employees table, ordered alphabetically. 

-- After creating the tables, we ADD an id column that serves as the PRIMARY KEY. 
-- We use the AUTO_INCREMENT constraint on the PRIMARY KEY to ensure that each id 
-- is assigned sequentially every time a new record is inserted.

-- The addition of a new column with the AUTO_INCREMENT constraint 
-- immediately assigns a sequential number to every existing row.

-- PART 1: CREATING CORE TABLES WITH NO SECONDARY TABLE

-- TABLE: ETHNICITIES

CREATE TABLE ethnicities AS (
    SELECT 
        race AS name
    FROM employees
    GROUP BY race
    ORDER BY race
);

ALTER TABLE ethnicities 
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

-- TABLE: GENDERS

CREATE TABLE genders AS (
    SELECT
        gender AS name
    FROM employees
    GROUP BY gender
    ORDER BY name
);

ALTER TABLE genders 
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

-- TABLE: LOCATIONS

CREATE TABLE locations AS (
    SELECT location AS name
    FROM employees
    GROUP BY location
    ORDER BY location
);

ALTER TABLE locations
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

-- PART 2: ADDING THE FOREIGN KEY COLUMNS TO THE FACT TABLE

-- Now that we have a few of our core tables, we establish relationships between these tables 
-- and the main "employees" table.

-- We begin by adding a new column to the employees table that will serve as a placeholder 
-- for our values. To ensure referential integrity, we define this column as a FOREIGN KEY, 
-- linking it to its corresponding PRIMARY KEY in the core tables.

-- In the code below, we add an ethnicity_id column to the employees table and define it as 
-- a FOREIGN KEY referencing the id column of the ethnicities table.

ALTER TABLE employees
ADD COLUMN ethnicity_id INT,
ADD CONSTRAINT fk_ethnicity FOREIGN KEY (ethnicity_id) REFERENCES ethnicities(id);

-- Now that we have our placeholder column in the employees table, we use an UPDATE statement 
-- with a correlated subquery to populate it with the corresponding id values from the 
-- ethnicities table.

-- The logic follows this approach: 
-- "From the ethnicities table, where the value in the name column matches the value in the 
-- race column of the employees table, assign the corresponding id from the ethnicities table 
-- to the ethnicity_id column in the employees table."

-- This process is similar to an XLOOKUP or VLOOKUP function in Excel, mapping values 
-- from one table to another using the ethnicity names.

UPDATE employees AS e
SET ethnicity_id = (
    SELECT id 
    FROM ethnicities AS et 
    WHERE et.name = e.race
);

-- We initially kept the race column in the employees table to facilitate the population 
-- of the ethnicity_id values. Now that this process is complete, we can safely drop 
-- the race column since we have replaced it with an ID reference.

ALTER TABLE employees
DROP COLUMN race;

-- This process is repeated for the other core tables: gender and location.

-- TABLE: GENDER

ALTER TABLE employees
ADD COLUMN gender_id INT,
ADD CONSTRAINT fk_gender FOREIGN KEY (gender_id) REFERENCES genders(id);

UPDATE employees AS e
SET e.gender_id = (SELECT id FROM genders AS g WHERE g.name = e.gender);

ALTER TABLE employees
DROP COLUMN gender;

-- TABLE: LOCATION

ALTER TABLE employees
ADD COLUMN location_id INT,
ADD CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES locations(id);

UPDATE employees AS e
SET location_id = (SELECT id FROM locations AS l WHERE l.name = e.location);

ALTER TABLE employees
DROP COLUMN location;

-- PART 3: CREATING OTHER CORE TABLES & SECONDARY RELATED TABLES

-- Since we are building a snowflake schema, this means that some tables will have related tables.
-- In our database, job titles will have a related departments table, and cities will have a related states table.

-- We structure the database this way to avoid storing repeated text strings in attributes, 
-- which would unnecessarily increase table size. This design choice also aligns with database normalization 
-- principles.

-- If we were to create a job titles table with a department column, each department name would be 
-- repeated for multiple job titles. Similarly, if we created a cities table with a state column, 
-- each state name would be repeated for multiple cities. 

-- This violates the First Normal Form (1NF), which advises against storing redundant values in a column.
-- To maintain a well-structured database, we ensure that each dimension table contains only unique values.

-- TABLE: DEPARTMENTS

CREATE TABLE departments AS (
    SELECT
        department AS name
    FROM employees
    GROUP BY name
    ORDER BY name
);

ALTER TABLE departments
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

-- Now, we must keep an incredibly IMPORTANT consideration in mind. Since we are creating 
-- secondary tables for core tables —such as job titles and departments— we need to determine 
-- whether job titles are unique across all records or if the same job title can exist in 
-- multiple departments.

-- If job titles are unique, we can simply create a job titles table with an auto-incremented 
-- primary key. However, if the same job title appears in multiple departments, the primary key 
-- for the job titles table must be a combination of job title and department to maintain uniqueness.

-- The following query checks for repeated job titles and their associated departments.
 
WITH titles AS (
    SELECT
        department,
        jobtitle
    FROM employees
    GROUP BY department, jobtitle
),
rep_titles AS (
    SELECT
        jobtitle,
        COUNT(*)
    FROM titles
    GROUP BY jobtitle
    HAVING COUNT(*) > 1
)

SELECT
    department,
    jobtitle
FROM titles
WHERE jobtitle IN (
    SELECT jobtitle
    FROM rep_titles
)
ORDER BY jobtitle, department;

-- Now we know that there are 37 job titles that exist in more than one department. For example:

-- Department                  Job Title
-- --------------------------- ---------------------
-- Business Development        Business Analyst
-- Engineering                 Business Analyst
-- Product Management          Business Analyst
-- Research and Development    Business Analyst

-- Since the same job title can exist in multiple departments, we must take this into account 
-- when establishing relationships between the core table (job titles) and the secondary table (departments).

CREATE TABLE jobtitles AS (
    SELECT
        department,
        jobtitle
    FROM employees
    GROUP BY department, jobtitle
    ORDER BY department, jobtitle
);

-- We now add the department_id column to the jobtitles table to serve as the foreign key 
-- that references the departments table.

ALTER TABLE jobtitles
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY,
ADD COLUMN department_id INT,
ADD CONSTRAINT fk_department FOREIGN KEY (department_id) REFERENCES departments(id);

-- We now populate the department_id column with the corresponding values from the departments table.

UPDATE jobtitles AS j
SET department_id = (SELECT id FROM departments AS d WHERE d.name = j.department);

-- We now add the jobtitle_id column to the employees table to serve as the foreign key 
-- that references the jobtitles table.

ALTER TABLE employees
ADD COLUMN jobtitle_id INT,
ADD CONSTRAINT fk_jobtitle FOREIGN KEY (jobtitle_id) REFERENCES jobtitles(id);

-- As we previously observed, a job title can exist in more than one department. 
-- This is why we created the jobtitles table based on a grouping of job titles 
-- and department names. 

-- This structure is crucial because when populating the jobtitle_id column in the employees table, 
-- we follow the same approach used for the ethnicity, gender, and location tables. 
-- However, in this case, the correlated subquery joins based on TWO conditions instead of one.

-- The logic follows:
-- "From the jobtitles table, where the jobtitle value in the jobtitle column matches 
-- the jobtitle value in the employees table AND the department value in the department 
-- column matches the department value in the employees table, assign the corresponding 
-- id from the jobtitles table to the jobtitle_id column in the employees table."

-- This process is similar to performing an XLOOKUP in Excel that matches based on two 
-- conditions instead of one.

UPDATE employees AS e
SET jobtitle_id = (
    SELECT id 
    FROM jobtitles AS j 
    WHERE j.jobtitle = e.jobtitle
    AND j.department = e.department
);

-- Now that we have populated the jobtitles table with the department_id values, 
-- we can safely drop the department column.

ALTER TABLE jobtitles
DROP COLUMN department;

-- Now that we have populated the employees table with jobtitle_id values using 
-- the jobtitle and department columns, we can safely drop them.

ALTER TABLE employees
DROP COLUMN department,
DROP COLUMN jobtitle;

-- ALL THE PREVIOUSLY APPLIED LOGIC TO THE DEPARTMENTS > JOBTITLES > EMPLOYEES TABLES 
-- IS ALSO APPLIED TO THE STATES > CITIES > EMPLOYEES TABLES.

-- We check if city names can exist in more than one state.

WITH state_city AS (
    SELECT
        location_state,
        location_city
    FROM employees
    GROUP BY location_state, location_city
    ORDER BY location_state, location_city
),
duplicate_cities AS (
    SELECT
        location_city,
        COUNT(location_city) AS total
    FROM state_city
    GROUP BY location_city
    HAVING total > 1
)

SELECT *
FROM state_city
WHERE location_city IN (
    SELECT location_city
    FROM duplicate_cities
)
ORDER BY location_city;

-- Now we know that there are 3 cities that exist in more than one state. For example:

-- State            City
-- ---------------  ---------------
-- Illinois        Bloomington
-- Indiana         Bloomington
-- Michigan        Warren
-- Ohio            Warren

-- This confirms that city names are not necessarily unique and can exist in multiple states.

-- Here, we create the states table.

CREATE TABLE states AS (
    SELECT 
        location_state AS name
    FROM employees
    GROUP BY location_state
    ORDER BY location_state
);

-- We add an id column to the states table.

ALTER TABLE states
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

-- Here, we create the cities table.

CREATE TABLE cities AS (
    SELECT
        location_state AS state,
        location_city AS name
    FROM employees
    GROUP BY location_state, location_city
    ORDER BY location_state, location_city
);

-- We add an id column to the cities table and a foreign key that references the states table.

ALTER TABLE cities
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST,
ADD COLUMN state_id INT,
ADD CONSTRAINT fk_state FOREIGN KEY (state_id) REFERENCES states(id);

-- We populate the state_id column in the cities table with the corresponding values from the states table.

UPDATE cities AS c
SET state_id = (SELECT id FROM states AS s WHERE c.state = s.name);

-- We add a city_id column to the employees table and define it as a foreign key 
-- that references the cities table.

ALTER TABLE employees
ADD COLUMN city_id INT,
ADD CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES cities(id);

-- We populate the city_id column in the employees table using a double condition.

UPDATE employees AS e
SET city_id = (
    SELECT id 
    FROM cities AS c 
    WHERE c.name = e.location_city 
    AND c.state = e.location_state
);

-- We drop the unneeded columns from the employees table.

ALTER TABLE employees
DROP COLUMN location_city,
DROP COLUMN location_state;

-- We drop the state column from the cities table since it is now referenced through state_id.

ALTER TABLE cities
DROP COLUMN state;

-- With this final update, we conclude the database structuring process. 

-- The employees table is now fully normalized, adhering to database design best practices. 
-- All redundant text columns have been replaced with foreign keys, ensuring data integrity 
-- and optimizing storage efficiency.

-- By implementing a snowflake schema, we have successfully:
-- 1. Separated core attributes into dedicated dimension tables.
-- 2. Established relationships between tables using primary and foreign keys.
-- 3. Eliminated duplicate values, improving query performance and maintainability.

-- This structured database is now optimized for seamless integration with Power BI, 
-- allowing for efficient reporting and analysis.