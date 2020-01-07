P = LOAD '$G' using PigStorage(',') AS (POINT:long,ADJ,long);
G = group P by ADJ;
C = foreach G generate group,COUNT($1);
O = ORDER C BY $1 DESC;
store O into '$O' using PigStorage(',');
