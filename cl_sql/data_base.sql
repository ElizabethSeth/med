create database card_data ;
GRANT SELECT, INSERT, CREATE TABLE ON card_data.* TO developper;
select * from card_data.large_patterns role_table_grants;
GRANT ALL ON *.* TO admin WITH GRANT OPTION;


CREATE TABLE card_data.hi_large (
                                    Timestamp String
                                    ,`From Bank` Int64
                                    ,Account String
                                    ,`To Bank` Nullable(Float32)
                                    ,`Account_1` Nullable(String)
                                    ,`Amount Received` Nullable(Float64)
                                    ,`Receiving Currency` Nullable(String)
                                    ,`Amount Paid` Nullable(Float64)
                                   ,`Payment Currency` Nullable(String)
                                    ,`Payment Format` Nullable(String)
                                    ,`Is Laundering` Nullable(Float64)
) ENGINE = MergeTree()
      ORDER BY Timestamp;
--DROP TABLE card_data.hi_large;

SELECT
    count(*)
FROM card_data.hi_large;


select
    parseDateTimeBestEffort(Timestamp)
from card_data.hi_large;

alter table card_data.hi_large
add column  Date DateTime;

alter table card_data.hi_large
update Date = parseDateTimeBestEffort(Timestamp)
where 1 = 1;

CREATE TABLE card_data.medium (
                                    Timestamp String
                                    ,`From Bank` Int64
                                    ,Account String
                                    ,`To Bank` Nullable(Float32)
                                    ,`Account_1` Nullable(String)
                                    ,`Amount Received` Nullable(Float64)
                                    ,`Receiving Currency` Nullable(String)
                                    ,`Amount Paid` Nullable(Float64)
                                   ,`Payment Currency` Nullable(String)
                                    ,`Payment Format` Nullable(String)
                                    ,`Is Laundering` Nullable(Float64)
) ENGINE = MergeTree()
      ORDER BY Timestamp;


CREATE table card_data.hi_small (
                                    Timestamp String
                                    ,`From Bank` Int64
                                    ,Account String
                                    ,`To Bank` Nullable(Float32)
                                    ,`Account_1` Nullable(String)
                                    ,`Amount Received` Nullable(Float64)
                                    ,`Receiving Currency` Nullable(String)
                                    ,`Amount Paid` Nullable(Float64)
                                   ,`Payment Currency` Nullable(String)
                                    ,`Payment Format` Nullable(String)
                                    ,`Is Laundering` Nullable(Float64)
) ENGINE = MergeTree()
      ORDER BY Timestamp;

--DROP TABLE  if exists card_data.hi_small;

CREATE TABLE card_data.large_patterns (
                                    Timestamp String,
                                    `From Bank` Int64,
                                    Account String,
                                    `To Bank` Nullable(Float32),
                                    `Account_1` Nullable(String),
                                    `Amount Received` Nullable(Float64),
                                    `Receiving Currency` Nullable(String),
                                    `Amount Paid` Nullable(Float64),
                                    `Payment Currency` Nullable(String),
                                    `Payment Format` Nullable(String),
                                    `Is Laundering` Nullable(Float64),
                                    `Anomaly Type` Nullable(String)
) ENGINE = MergeTree()
      ORDER BY Timestamp;
 show tables from card_data;

select * from card_data.large_patterns limit 300;

SELECT
    `From Bank`,
    SUM(`Amount Received`) OVER (PARTITION BY `From Bank`) AS TotalReceivedPerBank
FROM card_data.large_patterns;


SELECT
    `From Bank`
    ,Timestamp
    ,SUM(`Amount Received`) OVER (PARTITION BY `From Bank` ORDER BY Timestamp) AS CumulativeReceived
FROM card_data.large_patterns;

SELECT
    `Account_1`,
    COUNT(*) OVER (PARTITION BY `Account_1`) AS TransactionsPerReceiver
FROM card_data.large_patterns;

SELECT
    `From Bank`
    ,`Amount Received`
    ,ROW_NUMBER() OVER (PARTITION BY `From Bank` ORDER BY Timestamp) AS TransactionNumber
    ,DENSE_RANK() OVER (PARTITION BY `From Bank` ORDER BY `Amount Received` DESC) AS DenseTransactionRank
    ,RANK() OVER (PARTITION BY `From Bank` ORDER BY `Amount Received` DESC) AS TransactionRank
FROM card_data.large_patterns;

SELECT
    `Timestamp`
    ,`From Bank`
    ,`Amount Received`
    ,`Payment Currency`
    ,avg(`Amount Received`) OVER (partition by `From Bank`, `Payment Currency` order by `Timestamp` ) as avg_amount
    ,row_number() OVER (partition by `From Bank`, `Payment Currency` order by `Timestamp` ) as row_nb
from card_data.large_patterns
order by `From Bank`, `Receiving Currency`, `Timestamp`;


SELECT
    `Timestamp`,
    `From Bank`,
    `To Bank`,
    `Amount Received`,
    `Receiving Currency`,
    row_number() OVER (PARTITION BY `From Bank`, `Receiving Currency` ORDER BY `Timestamp`) AS transaction_rank
FROM card_data.large_patterns;



SELECT
    `Timestamp`,
    `From Bank`,
    `Anomaly Type`,
    `Amount Received`,
    SUM(`Amount Received`) OVER (PARTITION BY `From Bank`, `Anomaly Type` ORDER BY `Timestamp`) AS cumulative_sum
FROM card_data.large_patterns;


SELECT
    `Anomaly Type`,
    `From Bank`,
    `To Bank`,
    `Amount Received`,
    max(`Amount Received`) OVER (PARTITION BY `Anomaly Type`) AS max_received_by_anomaly
FROM card_data.large_patterns;





select `From Bank`
       ,`Anomaly Type`
       ,count(*) over (partition by `From Bank`, `Anomaly Type`) as total_anomalies_type
       ,row_number() over(partition by `From Bank`, `Anomaly Type` order by `Timestamp` asc ) as RowNumberType
       ,rank() over (partition by `From Bank`, `Anomaly Type` order by `Amount Received` desc) as RankType
FROM card_data.large_patterns
Where `Anomaly Type` is not null;



SELECT
    `Timestamp`,
    `From Bank`,
    `Anomaly Type`,
    `Amount Received`,
    SUM(`Amount Received`) OVER (
        PARTITION BY `From Bank`, `Anomaly Type`
        ORDER BY `Timestamp`
        rows between  5 preceding and 5 following
    ) AS cumulative_sum


FROM card_data.large_patterns;

SELECT
    toYear(parseDateTimeBestEffort(Timestamp)) AS Year,
    formatDateTime(parseDateTimeBestEffort(Timestamp), '%M') AS Month_Name,
    toDayOfMonth(parseDateTimeBestEffort(Timestamp)) AS Day
FROM card_data.large_patterns;

ALTER TABLE card_data.large_patterns
    ADD COLUMN Year Int32,
    ADD COLUMN Month_Name String,
    ADD COLUMN Day Int32;

ALTER TABLE card_data.large_patterns
UPDATE
    Year = toYear(parseDateTimeBestEffort(Timestamp)),
    Month_Name = formatDateTime(parseDateTimeBestEffort(Timestamp), '%M'),
    Day = toDayOfMonth(parseDateTimeBestEffort(Timestamp))
WHERE Timestamp IS NOT NULL;

SELECT Year, Month_Name, Day FROM card_data.large_patterns;

--range between UNBOUNDED PRECEDING AND CURRENT ROW






SELECT
    Year,
    Month_Name,
    Day,
    `Amount Received`,
    avg(`Amount Received`) OVER (
        PARTITION BY Year, Month_Name
        ORDER BY Day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weekly_avg_received
FROM card_data.large_patterns;



SELECT
    Year,
    Month_Name,
    Day,
    `Amount Paid`,
    SUM(`Amount Paid`) OVER (
        PARTITION BY Year, Month_Name
        ORDER BY Day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_monthly_paid
FROM card_data.large_patterns;

SELECT
    Year,
    Month_Name,
    Day,
    `Amount Paid`,
    SUM(`Amount Paid`) OVER (
        PARTITION BY Year, Month_Name
        ORDER BY Day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weekly_sum_paid
FROM card_data.large_patterns;

select
    Year
    ,Month_Name
    ,Day
    ,Account
    ,`Amount Received`
    , `Receiving Currency`
    ,sum(`Amount Received`-`Amount Paid`) over
        (partition by Year,Month_Name,Account,`Receiving Currency`
        order by Day
        ) as balance
from card_data.large_patterns;

select
    Year
    ,Month_Name
    ,Day
    ,Account
    ,`Payment Currency`
    ,balance
from (select
    Year
    ,Month_Name
    ,Day
     ,Account
     ,`Payment Currency`
    ,sum(`Amount Received`) as balance
from card_data.large_patterns
group by  Month_Name, Day ,`Payment Currency`, Account ,Year);
--order by Year, Month_Name, Day ,`Payment Currency`;



select
    *
from card_data.large_patterns
where `Amount Received`  > 2 * (
            select
                avg(`Amount Received`) as avg_amount
            from card_data.large_patterns
            where `Payment Currency` = 'US Dollar'
);



select
    *
from card_data.large_patterns l
where `Amount Received`  > 2 * (
    select
        avg(`Amount Received`) as avg_amount
    from card_data.large_patterns l_avg
    where `Payment Currency` = 'US Dollar' and l.Account = l_avg.Account
);

with  (
    select
        avg(`Amount Received`) as avg
    from card_data.large_patterns
    where `Payment Currency` = 'US Dollar'
) as avg_amount

select
    *
from card_data.large_patterns
where `Amount Received`  > 2 * avg_amount;



with avg_amount as (
    select
         Year
          ,Month_Name
          ,Day
          ,Account
          ,`Payment Currency`
          ,sum(`Amount Received`) as balance
     from card_data.large_patterns
     group by  Month_Name, Day ,`Payment Currency`, Account ,Year
)
,avg_year as (
    select
        avg(balance) as avg_balance
    from avg_amount
)

select
    Year
     ,Month_Name
     ,Day
     ,Account
     ,`Payment Currency`
     ,balance
from avg_amount
cross join avg_year;



select * from card_data.large_patterns limit 10;


WITH BankTransactions AS (
    SELECT
        `From Bank`
        ,`To Bank`
        ,COUNT(*) AS TransactionCount
    FROM card_data.large_patterns
    WHERE `From Bank` IS NOT NULL AND `To Bank` IS NOT NULL
    GROUP BY `From Bank`, `To Bank`
)
SELECT *
FROM BankTransactions
ORDER BY TransactionCount DESC;

WITH AccountPayments AS (
    SELECT
        Account,
        `Payment Currency`,
        SUM(`Amount Paid`) AS TotalPaid
    FROM card_data.large_patterns
    WHERE `Amount Paid` IS NOT NULL
    GROUP BY Account, `Payment Currency`
)
SELECT
    Account,
    TotalPaid,
    `Payment Currency`
FROM AccountPayments
ORDER BY TotalPaid DESC
LIMIT 10;

WITH LaunderingDays AS (
    SELECT
        Day,
        Month_Name,
        Year,
        COUNT(*) AS LaunderingCount
    FROM card_data.large_patterns
    WHERE `Is Laundering` > 0.5
    GROUP BY Day, Month_Name, Year
)
SELECT *
FROM LaunderingDays
ORDER BY LaunderingCount DESC
LIMIT 10;



WITH
    ReceivedSum AS (
        SELECT `Receiving Currency`,
               SUM(`Amount Received`) AS TotalReceived
        FROM card_data.large_patterns
        WHERE `Amount Received` IS NOT NULL
        GROUP BY `Receiving Currency`
    ),
    PaidSum AS (
        SELECT `Payment Currency`
             , SUM(`Amount Paid`) AS TotalPaid
        FROM card_data.large_patterns
        WHERE `Amount Paid` IS NOT NULL
        GROUP BY `Payment Currency`
    )
SELECT
    COALESCE(ReceivedSum.`Receiving Currency`, PaidSum.`Payment Currency`) AS Currency,
    TotalReceived,
    TotalPaid
FROM ReceivedSum
         FULL OUTER JOIN PaidSum
                         ON ReceivedSum.`Receiving Currency` = PaidSum.`Payment Currency`;


WITH previos_month as (
    select
        Year
        , Month_Name
        ,`From Bank`
        ,`To Bank`
         ,Account
        ,`Receiving Currency`
        ,`Amount Received`
        ,`Amount Paid`
        ,Day
      ,row_number() over (partition by `From Bank`, `To Bank`,Account, `Receiving Currency`, Year, Month_Name order by Year, Month_Name) as row_nb
    from card_data.large_patterns
),
    calc_deltas as (
        select
            `From Bank`
            ,`To Bank`
             ,Account
            ,`Receiving Currency`
            ,`Amount Paid`
            ,`Amount Received`
            , Day
            sum(`Amount Received`) over (partition by `From Bank`, `To Bank`,Account, `Receiving Currency`, Year, Month_Name
                order by Year, Month_Name
                rows between  30 preceding and  current row
                ) as total_received
            sum(`Amount Paid`) over (partition by `From Bank`, `To Bank`,Account, `Receiving Currency`, Year, Month_Name
            order by Year, Month_Name
            rows between  30 preceding and  current row
            ) as total_paid
            from previos_month



           LAG(`Amount Received`) OVER (PARTITION BY `From Bank`, `To Bank`,Account, `Receiving Currency`, Year, Month_Name ORDER BY Year, Month_Name) as prev_amount
           LAG(`Amount Paid`) OVER (PARTITION BY `From Bank`, `To Bank`,Account, `Receiving Currency`, Year, Month_Name ORDER BY Year, Month_Name) as prev_paid
       from previos_month
)
select
    `From Bank`
    ,`To Bank`
    ,Account
    ,`Receiving Currency`
    ,Day
    ,prev_amount
    , prev_paid
    IF(prev_amount > 0, (`Amount Received` - prev_amount) / prev_amount * 100) AS Received_percent_change
    IF(prev_paid > 0, (`Amount Paid` - prev_paid) / prev_paid * 100) AS Paid_percent_change
from calc_deltas;




select
    `From Bank`
     ,`To Bank`
     ,Account
     ,`Receiving Currency`
     ,`Amount Paid`
     ,`Amount Received`
     , Day
     ,Year

    LAG(`Amount Received`) OVER (PARTITION BY `From Bank`, `To Bank`,Account, `Receiving Currency`, Year, Month_Name ORDER BY Year, Month_Name) as prev_amount
    LAG(`Amount Paid`) OVER (PARTITION BY `From Bank`, `To Bank`,Account, `Receiving Currency`, Year, Month_Name ORDER BY Year, Month_Name) as prev_paid
from from card_data.large_patterns





select
    Year
     , Month_Name
     ,`From Bank`
     ,`To Bank`
     ,Account
     ,`Receiving Currency`
     ,`Amount Received`
     ,`Amount Paid`
     ,Day
     ,row_number() over (partition by `From Bank`, `To Bank`,Account, `Receiving Currency`, Year, Month_Name order by Year, Month_Name) as row_nb
from card_data.large_patterns;

select * from card_data.large_patterns limit 10;






with aggregate_data as (
    select
        Year
         ,Month_Name
         ,Account
         ,Day
         ,sum(`Amount Received`)  as total_received
         ,sum(`Amount Paid`) as total_paid

    from card_data.large_patterns
    group by Year, Month_Name, Day ,Account
)
select

             Account
            ,`Receiving Currency`
            ,`Amount Paid`
            ,`Amount Received`
            ,Day
            ,sum(`Amount Received`) over (partition by Account, `Receiving Currency`, Year, Month_Name
                order by Year, Month_Name

                rows between  30 preceding and  30 preceding
                ) as total_received
            ,sum(`Amount Paid`) over (partition by Account, `Receiving Currency`, Year, Month_Name
                order by Year, Month_Name
                rows between  30 preceding and  30 preceding
                ) as total_paid
from aggregate_data;





WITH aggregate_data AS (
    SELECT
        Year,
        Month_Name,
        Account,
        Day,
        `Receiving Currency`,
        SUM(`Amount Received`) AS total_received,
        SUM(`Amount Paid`) AS total_paid
    FROM card_data.large_patterns
    GROUP BY Year, Month_Name, Day, Account, `Receiving Currency`
),
     monthly_data AS (
         SELECT
             Account,
             `Receiving Currency`,
             total_paid AS Amount_Paid,
             total_received AS Amount_Received,
             Day,
             Year,
             Month_Name,
             SUM(total_received) OVER (
                 PARTITION BY Account, `Receiving Currency`, Year, Month_Name
                 ORDER BY Day
                 ROWS BETWEEN 30 PRECEDING AND 30 PRECEDING
                 ) AS previous_month_received,
             SUM(total_paid) OVER (
                 PARTITION BY Account, `Receiving Currency`, Year, Month_Name
                 ORDER BY Day
                 ROWS BETWEEN 30 PRECEDING AND 30 PRECEDING
                 ) AS previous_month_paid
         FROM aggregate_data
     )
SELECT
    Account,
    `Receiving Currency`,
    Amount_Paid,
    Amount_Received,
    Day,
    Year,
    Month_Name,
    previous_month_received,
    previous_month_paid,
    (Amount_Received / IF(previous_month_received = 0, 1, previous_month_received)) * 100 AS received_percentage,
    (Amount_Paid / IF(previous_month_paid = 0, 1, previous_month_paid)) * 100 AS paid_percentage
FROM monthly_data;




select * from card_data.large_patterns limit 10;