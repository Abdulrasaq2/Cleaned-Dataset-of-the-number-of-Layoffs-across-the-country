select *
from layoffs
;

create table layoffs1
like layoffs;

select *
from layoffs1;

insert into layoffs1
select *
from layoffs;

select *, 
row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs1;

with duplicate_cte as
(
select *, 
row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs1
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs1;



CREATE TABLE `layoffs2` (
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
from layoffs2;

insert into layoffs2
select *, 
row_number() over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs1;

select *
from layoffs2
where row_num > 1;


delete
from layoffs2
where row_num > 1;

select *
from layoffs2;

#standard data

select distinct company, trim(company)
from layoffs2;

update layoffs2
set company = trim(company);

select distinct industry
from layoffs2
order by 1;

select *
from layoffs2
where industry like 'Crypto%';

update layoffs2
set industry = 'Crypto'
where industry like 'Crypto%';


select distinct country, trim(trailing '.' from country)
from layoffs2
where country like 'United States%'
order by 1;

update layoffs2
set country = trim(trailing '.' from country);

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs2;

update layoffs2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select *
from layoffs2;

alter table layoffs2
modify column `date` date;


select *
from layoffs2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs2
where industry is null
or industry = '';

update layoffs2
set industry = null
where industry = '';


select *
from layoffs2  l2
join layoffs2 l3
	on l2.company = l3.company
where (l2.industry is null or l2.industry = '')
and l3.industry is not null;

update layoffs2  l2
join layoffs2 l3
	on l2.company = l3.company
set l2.industry = l3.industry
where (l2.industry is null)
and l3.industry is not null;



delete
from layoffs2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs2
drop column row_num;







































































































