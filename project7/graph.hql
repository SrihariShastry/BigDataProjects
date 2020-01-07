drop table Points;

create table Points (
  point bigint,
  adj bigint)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Points;

select adj,count(1) as c
from Points
group by Points.adj
order by c DESC;