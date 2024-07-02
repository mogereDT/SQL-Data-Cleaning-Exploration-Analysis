-- Data Cleaning Project

#This data contains data about layoffs across the world as from 2022
#I will clean this data to prepare it for data exploration

#The steps taken for the data cleaning are listed as below:

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Check for Null values and decide hiw to deal with them
-- 4. Remove Any Columns that are irrelevant e.g those completely blank

#to work well, I'll create a copy of the raw data so that I work with a copy of the raw data, just in case i need the initial version

create table layoffs_staging
like layoffs;

select * from layoffs_staging;
insert layoffs_staging
select *
from layoffs;

#Check for duplicates
select *,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_staging;

#Create a cte to help identify duplicates
with duplicate_cte as
(select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging 
)
select *
from duplicate_cte
where row_num  >1; #This brings up all the records that have duplicates

#Check a case of Casper to confirm if the querry correctly idenitfies duplicates

select*
from layoffs_staging
where company='Casper';#wthere seems to be two duplicates
#To help remove duplicates, by deleting one of the duplicates
#however, since i am working with mysql, the server does not support deleting from the cte above

delete 
from duplicate_cte
where row_num > 1;#this prompts an error unlike postgresql and microsft sql server


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;#creates an empty table

#Now insert the data from the cte above into the layoff_staging2 table
insert into layoffs_staging2
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1;#brings up duplicates

#now remove the duplicates
delete 
from layoffs_staging2
where row_num >1;

#check if the duplicates still exist
select *
from layoffs_staging2
where row_num > 1;#brings up an empty table, hence duplicates deleted

-- Standardizing the data
#Try find issues in the data then fix them
#have a look at the companies within the data

select
distinct(company)
from layoffs_staging2;#the centries in the company column have a trailing white space at the beggining
#To remove, I will utilize the trim fucntion 

update layoffs_staging2
set company = TRIM(company);#works and changes 11 rows  

#Check the industry column
select distinct industry
from layoffs_staging2
order by 1;#has null rows, blank rows, also some entries such as "Crypto" and "Crypto Currency" appear as unique different fiels when in fact they are the same industry

#Have a look at the Crypto industry
select *
from layoffs_staging2
where industry like 'Crypto%';

#Set all the records under crypto as 'Crypto'
update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';#corrects the 3 instances in the industry column that were having typos

#Have a look at the location column
select distinct location
from layoffs_staging2
order by 1;#looks good

#have a look at the country column
select distinct country
from layoffs_staging2
order by 1;#the united states has two versions "United States" and "United States."

#fix the country column
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) AS trimmed_country
FROM layoffs_staging2
ORDER BY country;

update layoffs_staging2
set country = TRIM(TRAILING '.' FROM country)
where country like 'United States%';#fixes the country column

# Next fix the date column as it is currently stored as text field
update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y'); 

alter table layoffs_staging2
modify column `date` date;#correctly fixed

#Next, deal with nulls and blanks
#start with the industry column since it had some nulls and blanks
 select * 
 from layoffs_staging2
 where industry is null or 
 industry = '';
 
 #have a look at a case of AirBnb
 select *
 from layoffs_staging2
 where company='Airbnb';#has two records with on ehaving the insutry blank and the other indicating 'trave;'
 #to foix this, I need to update teh blanks for such companies as Airbnb with the rightful industry.
 #So is the case for Carvana, Juul,etc
 
 #A self join will help if on one table to see which table has a blanlk on the industry column, and then replace the blanks with the non-blank table
 
 #first, set the blanks to null
 
 update layoffs_staging2
 set industry = null
 where industry = '';
 #now do the self join
 update layoffs_staging2 t1
 join layoffs_staging2 t2
	on t1.company=t2.company
 set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;
 
 #Confirm this worked
  select * 
 from layoffs_staging2
 where industry is null or 
 industry = '';#just Bally's has nulls still, because it didn't have two cases with one being null and the other having an entry as the case of AirBnb
 
 #The total_laid_off and percentage_laid_off have comple nas, but it is hard to deal with since we don't have the columns showing total employees
#HI will delete such records since the data provides no value to the overall project since it provides no information of the percenatge laid off, or total laid off

delete 
from layoffs_staging2
where total_laid_off is null 
and
percentage_laid_off is null; #deleted 361 records

#since I am done with the row_num column, then i drop it

alter table layoffs_staging2
drop column row_num;#dropped

-- END!

#Next, I will use the cleaned data to conduct exploraory data analysis