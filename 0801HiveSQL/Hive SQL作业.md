如果图片不显示， 请 git clone到本地进行查看

git clone git@github.com:wanghuan2054/geektime.git

Hive作业

目录： 

[toc]

## Hive 建表

```sql
create table hive_sql_test1.t_user(
userid bigint,
sex String,
age int,
occupation String,
zipcode String
)
row format delimited fields terminated by ':';
load data local inpath '/home/hive/users.dat'  overwrite into  table  hive_sql_test1.t_user;


create table hive_sql_test1.t_movie
(
movieid bigint,
moviename string,
movietype string
)
row format delimited fields terminated by ':';

load data local inpath '/home/hive/movies.dat' overwrite into table hive_sql_test1.t_movie;


create table hive_sql_test1.t_rating
(
userid bigint,
movieid bigint,
rate double,
times  string
)
row format delimited fields terminated by ':';
load data local inpath '/home/hive/ratings.dat' overwrite into table hive_sql_test1.t_rating;
```



## 题目1 

简单：展示电影ID为2116这部电影各年龄段的平均影评分

### 解法1 （不考虑数据倾斜）

不考虑数据倾斜问题

#### SQL

```sql
SELECT t.age , avg(t1.rate) AS AVGRATE from hive_sql_test1.t_user t 
INNER JOIN hive_sql_test1.t_rating t1 
ON (T.userid = T1.userid)
WHERE t1.movieid = 2116 
GROUP BY T.age
ORDER BY T.age ; 
```

#### 运行结果

![image-20210802194530052](images/image-20210802194530052.png)

### 解法2（考虑数据倾斜）

考虑数据倾斜问题，假设2116这部电影评分记录集中在28-30这个年龄段,其它年龄段寥寥无几

解决数据倾斜的方法很多，本文列出我的一条思路

1. 首先按照age和occupation 或者 age 和zipcode 进行分组 ， 对原先按照age分组所有age相同的数据会同时shuffle到一个reduce节点，按照新分组会将之前倾斜值的倾斜程度变为原来的1/N ， N=（occupation  或者zipcode 值）%reducer 的值的不同个数

2. 将1的子查询结果（GROUP BY T.age ,t.zipcode  预聚合 ），之后再统一对age进行分组求avg ， 降低了之前一个节点处理所有数据量。

   核心思路，分治思想，对任务进行拆分，先多字段分组求和， 后将中间聚合结果统一求avg。 

#### SQL

```sql
SELECT X.age as age , avg(X.sumrate) as avgrate FROM 
(
SELECT t.age , t.zipcode ,  sum(t1.rate) AS sumrate from hive_sql_test1.t_user t 
INNER JOIN hive_sql_test1.t_rating t1 
ON (T.userid = T1.userid)
WHERE t1.movieid = 2116 
GROUP BY T.age ,t.zipcode 
) X
GROUP BY X.age 
ORDER BY X.age ; 
```

## 题目2

中等：找出男性评分最高且评分次数超过50次的10部电影，展示电影名，平均影评分和评分次数

### SQL

```sql
SELECT t.sex AS sex,
       t2.moviename AS name,
       avg(t1.rate) AS avgrate,
       count(*) AS total
FROM hive_sql_test1.t_user t
INNER JOIN hive_sql_test1.t_rating t1 ON (T.userid = T1.userid)
INNER JOIN hive_sql_test1.t_movie t2 on(t1.movieid = t2.movieid)
WHERE t.sex = 'M'
GROUP BY T.sex,
         t2.moviename
HAVING count(*) > 50
ORDER BY avgrate DESC
LIMIT 10 ;
```

### 运行结果

![image-20210802210742574](images/image-20210802210742574.png)



## 题目3(选做)

困难：找出影评次数最多的女士所给出最高分的10部电影的平均影评分，展示电影名和平均影评分（可使用多行SQL）

### 写法1 

嵌套子查询

#### SQL

```sql
SELECT t3.moviename,
       avg(t2.rate) AS avgrate
FROM (SELECT a.movieid , a.rate
   FROM  (SELECT t.userid,
          count(*) AS total
   FROM hive_sql_test1.t_user t
   INNER JOIN hive_sql_test1.t_rating t1 ON (T.userid = T1.userid)
   WHERE t.sex = 'F'
   GROUP BY T.userid
   ORDER BY total DESC
   LIMIT 1) x
   inner join hive_sql_test1.t_rating a on (x.userid = a.userid)
   ORDER BY a.rate DESC
   LIMIT 10)  t1
INNER JOIN hive_sql_test1.t_rating t2 on(t1.movieid = t2.movieid)
INNER JOIN hive_sql_test1.t_movie t3 on(t2.movieid = t3.movieid)
GROUP BY t2.movieid , t3.moviename
ORDER BY avgrate DESC ;
```

#### 运行结果

![image-20210803180848388](images/image-20210803180848388.png)

![image-20210803180929956](images/image-20210803180929956.png)

### 写法2

WITH ..... AS .....

#### SQL

```sql
-- 找出影评次数最多的女士
WITH Rating_CNT AS
  (SELECT t.userid,
          count(*) AS total
   FROM hive_sql_test1.t_user t
   INNER JOIN hive_sql_test1.t_rating t1 ON (T.userid = T1.userid)
   WHERE t.sex = 'F'
   GROUP BY T.userid
   ORDER BY total DESC
   LIMIT 1), 
-- 找出该女士评分最高的10部电影
TOPMOVIES AS
  (SELECT a.movieid , a.rate
   FROM  Rating_CNT x
   inner join hive_sql_test1.t_rating a on (x.userid = a.userid)
   ORDER BY a.rate DESC
   LIMIT 10) 
   
-- select t2.* from TOPMOVIES t2;

-- 求这10部电影的平均影评分，展示电影名和平均影评分
SELECT t3.moviename,
       avg(t2.rate) AS avgrate
FROM TOPMOVIES  t1
INNER JOIN hive_sql_test1.t_rating t2 on(t1.movieid = t2.movieid)
INNER JOIN hive_sql_test1.t_movie t3 on(t2.movieid = t3.movieid)
GROUP BY t2.movieid , t3.moviename
ORDER BY avgrate DESC ;
```

#### 运行结果

![image-20210803174100304](images/image-20210803174100304.png)



![image-20210803174200784](images/image-20210803174200784.png)



## GeekFileFormat实现

项目源码位置

