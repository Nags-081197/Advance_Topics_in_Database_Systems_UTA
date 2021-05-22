drop table Input_table;

drop table Neighbours_Count_table;

drop table Counts_Clustering_table;

create table Input_table (
  a BIGINT,
  b BIGINT)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Input_table;

create table Neighbours_Count_table (
  a BIGINT,
  b BIGINT)
row format delimited fields terminated by ',' stored as textfile;

create table Counts_Clustering_table (
  a BIGINT,
  b BIGINT)
row format delimited fields terminated by ',' stored as textfile;

INSERT OVERWRITE TABLE Neighbours_Count_table
SELECT a,count(*)
FROM Input_table
GROUP BY a;

INSERT OVERWRITE TABLE Counts_Clustering_table
SELECT b,count(*)
FROM Neighbours_Count_table
GROUP BY b;

select a, b
from Counts_Clustering_table;