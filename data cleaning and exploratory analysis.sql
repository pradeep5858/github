






with duplicate_cte as 
(
select * ,
 row_number() over 
(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)

delete from duplicate_cte
where row_num >1;


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
  `row_num`  int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs_staging2
select *,row_number() over(partition by company,location,industry,total_laid_off,
 percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging;
 
select  country from layoffs_staging2
where country like 'United States%';

 
 update layoffs_staging2
 set company=trim(company);
 
 update layoffs_staging2
 set `date`= str_to_date(`date`, '%m/%d/%Y');
 
 update layoffs_staging2
 set country=trim(trailing '.'  from country)
 where country like 'United States%';
 
update layoffs_staging2
set industry='Crypto'
 where industry like 'Crypto%';
 
 select industry from layoffs_staging2
 where industry like 'Crypto%';
 
 alter table layoffs_staging2
 modify column `date` date;
 
 select distinct industry from layoffs_staging2
 order by 1;
 
 select * from layoffs_staging2
 where company='Airbnb';
 
 update layoffs_staging2
 set industry=null
 where industry='';
 
 update layoffs_staging2 t1
 join layoffs_staging2 t2
   on t1.company=t2.company
 set t1.industry=t2.industry
 where t1.industry is null
 and t2.industry is not null;
 
 select * from layoffs_staging2
 where total_laid_off is null and 
 percentage_laid_off is null;
 
 delete  from layoffs_staging2
 where total_laid_off is null and 
 percentage_laid_off is null;
 
 alter table layoffs_staging2
 drop column row_num;
 
 -- EDA :
 
select max(total_laid_off), max(percentage_laid_off) 
from layoffs_staging2;

select * from layoffs_staging2;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select month(`date`), sum(total_laid_off)
from layoffs_staging2
group by month(`date`);


with Rolling_total as
(select substring(`date`,1,7) as `month`,  sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc)

select `month`,total_off, sum(total_off) over (order by `month`) as rolling_total
from Rolling_total;



select substring(`date`,1,7) as `month`,  sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;


select * from layoffs_staging2;


with Company_year(company,years,total_laid_off) as
(select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), company_newrank as
(
select *,
dense_rank() over(partition by years order by total_laid_off desc) as ranking
 from company_year
 where years is not null
)
select * from company_newrank
where ranking<=5
;




select * from layoffs_staging2;

with cte_company(company,years,total_laid_off) as 
(
select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by  company, year(`date`)
), company2 as(
select * , dense_rank() over (partition by years order by total_laid_off desc) as ranking
from cte_company
where years is not null)

select * from company2
where ranking<=5
order by ranking asc;











 
 
 
  















