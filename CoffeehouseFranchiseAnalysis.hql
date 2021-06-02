--Francisco Aceves
-- Project 1: Coffeehose Franchise Analysis
----------

use project1;

--Branches table

CREATE TABLE IF NOT EXISTS 
branches(beverage string, branch string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/francisco/project1/datasets/Bev_BranchA.txt' INTO TABLE branches;
LOAD DATA INPATH '/user/francisco/project1/datasets/Bev_BranchB.txt' INTO TABLE branches;
LOAD DATA INPATH '/user/francisco/project1/datasets/Bev_BranchC.txt' INTO TABLE branches;


--Counts table

CREATE TABLE IF NOT EXISTS 
counts(beverage string, counts string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/francisco/project1/datasets/Bev_ConscountA.txt' INTO TABLE counts;
LOAD DATA INPATH '/user/francisco/project1/datasets/Bev_ConscountB.txt' INTO TABLE counts;
LOAD DATA INPATH '/user/francisco/project1/datasets/Bev_ConscountC.txt' INTO TABLE counts;


--***************Problem Scenario 1 


--What is the total number of consumers for Branch1
Select SUM(c.counts) as total  
FROM Branches b 
FULL OUTER JOIN Counts c 
ON (b.beverage = c.beverage) 
WHERE b.Branch = 'Branch1';


--What is the number of consumers for the Branch2?
Select SUM(c.counts) as total  
FROM Branches b 
FULL OUTER JOIN Counts c 
ON (b.beverage = c.beverage) 
WHERE b.Branch = 'Branch2';


--**********Problem Scenario 2 


--What is the most consumed beverage on Branch1?

SELECT b.beverage, SUM(c.counts) as total  
FROM Branches b 
FULL OUTER JOIN 
Counts c ON (b.beverage = c.beverage) 
WHERE b.Branch = 'Branch1' 
GROUP BY b.beverage 
ORDER BY total DESC 
LIMIT 1;

--What is the least consumed beverage on Branch2

SELECT b.beverage, SUM(c.counts) as total  
FROM Branches b 
FULL OUTER JOIN 
Counts c ON (b.beverage = c.beverage) 
WHERE b.Branch = 'Branch2' 
GROUP BY b.beverage 
ORDER BY total ASC
LIMIT 1;

--*****************Problem Scenario 3


--What are the beverages available on Branch10, Branch8, and Branch1?

Select DISTINCT Beverage
FROM Branches
WHERE Branch = 'Branch10' OR Branch = 'Branch8' OR Branch = 'Branch1';

--what are the common beverages available in Branch4,Branch7?

--version 1
SELECT DISTINCT b.beverage 
FROM branches b
WHERE b.branch = 'Branch4' AND b.beverage IN 
(SELECT a.Beverage FROM branches a WHERE a.branch = 'Branch7')
ORDER BY b.beverage;


--version 2 
SELECT a.beverage FROM branches a where branch = 'Branch4' 
INTERSECT 
SELECT b.beverage FROM branches b where b.branch = 'Branch7';


--**************Problem Scenario 4
--create a partition,index,View for the scenario3.


--What are the beverages available on Branch10, Branch8, and Branch1?

--partition
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.dynamic.partition=true;

CREATE TABLE BranchesPart (beverage String) 
PARTITIONED BY (branch String) 
row format delimited fields terminated by ',' 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE BranchesPart 
PARTITION (branch) 
SELECT beverage,branch FROM branches;

show partitions branchespart;
select * from branchespart;

--view
CREATE VIEW IF NOT EXISTS branch10_8_1 AS
SELECT DISTINCT beverage 
FROM branchespart 
WHERE branch = 'Branch10' OR branch = 'Branch8' OR branch = 'Branch1';

show views;
select* from branch10_8_1 ;

--index
CREATE INDEX idx_bevbr ON TABLE branchespart(beverage)  
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;
ALTER INDEX idx_bevbr ON  branchespart REBUILD;

CREATE INDEX idx_bevcounts ON TABLE counts(beverage) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler';
ALTER INDEX idx_bevcounts ON counts REBUILD;

show index on branchespart;
show index on counts;

--what are the common beverages available in Branch4,Branch7?

-view
CREATE VIEW IF NOT EXISTS common_bev4_7 AS
SELECT DISTINCT b.beverage 
FROM branches b
WHERE b.branch = 'Branch4' AND b.beverage IN 
(SELECT a.Beverage FROM branches a WHERE a.branch = 'Branch7')
ORDER BY b.beverage;

select * from common_bev4_7;



--************Problem Scenario 5
--Alter the table properties to add "note","comment"



ALTER TABLE branchespart SET TBLPROPERTIES('note' = 'Table is the base branch table just partitioned according to branch');

ALTER TABLE counts SET TBLPROPERTIES('TableFor' = 'Consumer Counts for Beverages')

show tblproperties branchespart;
show tblproperties counts;



--************* Problem Scenario 6
--Remove the row 5 from the output of Scenario 3


/**
 * 
 * SQL Processes the query in the following order:
	FROM
	WHERE
	GROUP BY
	HAVING
	SELECT
	ORDER BY
	TOP
	SO WE need a subquery in the FROM with the window function of the ROW_NUMBER
	https://www.toptal.com/sql/intro-to-sql-windows-functions
 */


Select *
FROM (
	SELECT *, ROW_NUMBER() OVER() AS rowNum
	FROM branchespart 
	) as temp
WHERE rowNum !=5;


--writing to hdfs
INSERT OVERWRITE DIRECTORY '/user/francisco/test' ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS TEXTFILE
Select *
FROM (
	SELECT *, ROW_NUMBER() OVER() AS rowNum
	FROM branchespart 
	) as temp
WHERE rowNum !=5;

