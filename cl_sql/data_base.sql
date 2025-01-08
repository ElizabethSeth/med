create database card_data ;
GRANT SELECT, INSERT, CREATE TABLE ON card_data.* TO developper;
select * from card_data.large_patterns role_table_grants;
GRANT ALL ON *.* TO admin WITH GRANT OPTION;

-- Create a table named "hi_large" in the "card_data" schema with a specified structure for storing financial transaction data.
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



-- Convert the Timestamp string field to DateTime format using the best-effort parser.
select
    parseDateTimeBestEffort(Timestamp)
from card_data.hi_large;


-- Convert the Timestamp string field to DateTime format using the best-effort parser.
alter table card_data.hi_large
add column  Date DateTime;

-- Update the "Date" column with parsed DateTime values from the "Timestamp" column.
alter table card_data.hi_large
update Date = parseDateTimeBestEffort(Timestamp)
where 1 = 1;


-- Create another table "medium" with a similar structure to "hi_large" for potentially storing a different set of transactions
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


-- Create a table "large_patterns" with an additional column "Anomaly Type" to track anomalies.
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

-- Using window functions to calculate the total amount received per "From Bank" (grouped by bank)
SELECT
    `From Bank`,
    SUM(`Amount Received`) OVER (PARTITION BY `From Bank`) AS TotalReceivedPerBank
FROM card_data.large_patterns;

-- Calculate the cumulative amount received by each "From Bank" ordered by Timestamp
SELECT
    `From Bank`
    ,Timestamp
    ,SUM(`Amount Received`) OVER (PARTITION BY `From Bank` ORDER BY Timestamp) AS CumulativeReceived
FROM card_data.large_patterns;

-- Count the number of transactions per "Account_1" (receiver) using window functions.
SELECT
    `Account_1`,
    COUNT(*) OVER (PARTITION BY `Account_1`) AS TransactionsPerReceiver
FROM card_data.large_patterns;

SELECT
    `From Bank`
    ,`Amount Received`
    ,row_number() OVER (PARTITION BY `From Bank` ORDER BY Timestamp) AS TransactionNumber
    ,dense_rank() OVER (PARTITION BY `From Bank` ORDER BY `Amount Received` DESC) AS DenseTransactionRank
    ,rank() OVER (PARTITION BY `From Bank` ORDER BY `Amount Received` DESC) AS TransactionRank
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
    `Timestamp`
    ,`From Bank`
    ,`Anomaly Type`
    ,`Amount Received`
    ,SUM(`Amount Received`) OVER (PARTITION BY `From Bank`, `Anomaly Type` ORDER BY `Timestamp`) AS cumulative_sum
FROM card_data.large_patterns;


SELECT
    `Anomaly Type`
    ,`From Bank`
    ,`To Bank`
    ,`Amount Received`
    ,max(`Amount Received`) OVER (PARTITION BY `Anomaly Type`) AS max_received_by_anomaly
FROM card_data.large_patterns;




-- Count anomalies per "From Bank" and "Anomaly Type", and assign row numbers ordered by "Timestamp" and ranks ordered by "Amount Received"
select `From Bank`
       ,`Anomaly Type`
       ,count(*) over (partition by `From Bank`, `Anomaly Type`) as total_anomalies_type
       ,row_number() over(partition by `From Bank`, `Anomaly Type` order by `Timestamp` asc ) as RowNumberType
       ,rank() over (partition by `From Bank`, `Anomaly Type` order by `Amount Received` desc) as RankType
FROM card_data.large_patterns
Where `Anomaly Type` is not null;



-- Extract the year, month, and day from the "Timestamp" and display the extracted values.
SELECT
    toYear(parseDateTimeBestEffort(Timestamp)) AS Year,
    formatDateTime(parseDateTimeBestEffort(Timestamp), '%M') AS Month_Name,
    toDayOfMonth(parseDateTimeBestEffort(Timestamp)) AS Day
FROM card_data.large_patterns;

ALTER TABLE card_data.large_patterns
    ADD COLUMN Year Int32,
    ADD COLUMN Month_Name String,
    ADD COLUMN Day Int32;
-- Update the "Year", "Month_Name", and "Day" columns using the parsed values from the "Timestamp"
ALTER TABLE card_data.large_patterns
UPDATE
    Year = toYear(parseDateTimeBestEffort(Timestamp)),
    Month_Name = formatDateTime(parseDateTimeBestEffort(Timestamp), '%M'),
    Day = toDayOfMonth(parseDateTimeBestEffort(Timestamp))
WHERE Timestamp IS NOT NULL;




-- Calculate a rolling sum of "Amount Received" within a window of 5 preceding and 5 following rows for each "From Bank" and "Anomaly Type".
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
    coalesce(ReceivedSum.`Receiving Currency`, PaidSum.`Payment Currency`) AS Currency,
    TotalReceived,
    TotalPaid
FROM ReceivedSum
         FULL OUTER JOIN PaidSum
                         ON ReceivedSum.`Receiving Currency` = PaidSum.`Payment Currency`;




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




WITH aggregate_data AS (
    SELECT
        Year,
        Month_Name,
        Day,
        Account,
        `Receiving Currency`,
        SUM(`Amount Received`) AS total_received,
        SUM(`Amount Paid`) AS total_paid
    FROM card_data.large_patterns
    GROUP BY Year, Month_Name, Day, Account, `Receiving Currency`
)
SELECT
    Account,
    `Receiving Currency`,
    Year,
    Month_Name,
    Day,
    SUM(total_received) OVER (
        PARTITION BY Account, `Receiving Currency`, Year, Month_Name
        ORDER BY Year, Month_Name, Day
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS rolling_total_received,
    SUM(total_paid) OVER (
        PARTITION BY Account, `Receiving Currency`, Year, Month_Name
        ORDER BY Year, Month_Name, Day
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS rolling_total_paid
FROM aggregate_data
ORDER BY `Receiving Currency`, Year, Month_Name, Day;

select * from card_data.large_patterns limit 10;





SELECT
    Account,
    COUNT(*) AS repetitions
FROM
    card_data.large_patterns
GROUP BY
    Account
ORDER BY
    repetitions DESC;



SELECT
    Account
FROM
    card_data.large_patterns
WHERE match(Account, '^.{8,10}$');

SELECT
    *
FROM
    card_data.large_patterns

WHERE match(Account, '^[A-Z0-9]+$');

SELECT
    *
FROM
    card_data.large_patterns
WHERE match(Account, '^[0-9]+$');

SELECT
    *
FROM
    card_data.large_patterns
WHERE match(Account, '^81.*60$');


SELECT
    Account,
    SUM(1) OVER (PARTITION BY match(Account, 'D3') ORDER BY Account ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum,
    COUNT(*) OVER (PARTITION BY match(Account, 'D3')) AS total_count
FROM
    card_data.large_patterns
WHERE
    match(Account, 'D3');

SELECT
    `Receiving Currency`,
    COUNT(DISTINCT Account) AS unique_accounts_per_currency
FROM
    card_data.large_patterns
WHERE
    match(Account, '^82.*B0$')
GROUP BY
    `Receiving Currency`;


SELECT
    Account,
    `Receiving Currency`,
    dense_rank() OVER (PARTITION BY match(Account, '^[A-Z0-9]+$'), `Receiving Currency` ORDER BY Account) AS dense_rank_per_currency
FROM
    card_data.large_patterns
WHERE
    match(Account, '^[A-Z0-9]+$');








ALTER TABLE card_data.large_patterns
    ADD COLUMN `From Bank Code` Nullable(String);

SELECT
    extract(`From Bank`, '^(\\d{3})') AS `From Bank Code`
FROM card_data.large_patterns;











ALTER TABLE card_data.large_patterns
    DROP COLUMN `Time Category`;

ALTER TABLE card_data.large_patterns
    ADD COLUMN `Time Category` Nullable(String);




SELECT
    database,
    table,
    mutation_id,
    command,
    parts_to_do,
    is_done
FROM system.mutations
WHERE table = 'large_patterns';


SELECT
    Timestamp,
    parseDateTimeBestEffort(replace(Timestamp, '/', '-')) AS ParsedTimestamp
FROM card_data.large_patterns
LIMIT 100;

ALTER TABLE card_data.large_patterns
UPDATE `Time Category` = CASE
                             WHEN toHour(parseDateTimeBestEffort(replace(Timestamp, '/', '-'))) BETWEEN 6 AND 12 THEN 'Morning'
                             WHEN toHour(parseDateTimeBestEffort(replace(Timestamp, '/', '-'))) BETWEEN 12 AND 18 THEN 'Afternoon'
                             WHEN toHour(parseDateTimeBestEffort(replace(Timestamp, '/', '-'))) BETWEEN 18 AND 24 THEN 'Evening'
                             ELSE 'Night'
    END
WHERE Timestamp IS NOT NULL;

SELECT
    Timestamp,
    `Time Category`
FROM card_data.large_patterns
LIMIT 100;


SELECT
    Timestamp,
    parseDateTimeBestEffort(replace(Timestamp, '/', '-')) AS ParsedTimestamp
FROM card_data.large_patterns
LIMIT 100;





SELECT
    mutation_id,
    command,
    is_done,
    parts_to_do
FROM system.mutations
WHERE table = 'large_patterns';

KILL MUTATION WHERE mutation_id = 'mutation_3.txt';
KILL MUTATION WHERE mutation_id = 'mutation_7.txt';
KILL MUTATION WHERE mutation_id = 'mutation_8.txt';





ALTER TABLE card_data.large_patterns ADD COLUMN Account_Type String;

select * from card_data.large_patterns limit 10;


ALTER TABLE card_data.large_patterns
UPDATE Account_Type = CASE
                          WHEN match(Account, '^[0-9]+$') THEN 'Numeric'
                          WHEN match(Account, '^[A-Z]+$') THEN 'Alphabetic'
                          WHEN match(Account, '^[A-Z0-9]+$') THEN 'Alphanumeric'
                          ELSE 'Other'
    END
WHERE Account IS NOT NULL;





select
    --'hiiiiiiiii' as st,
    extractAll('hhiiiiiiiii', '^h.+.') as match_result;






SELECT
    Account
    ,SUM(1) OVER (PARTITION BY match(Account, 'D3'))AS cumulative_sum
    ,COUNT(*) OVER (PARTITION BY match(Account, 'D3')) AS total_count
    ,SUM(1) OVER (PARTITION BY match(Account, 'D3') order by Account) AS cumulative_sum_order
    ,COUNT(*) OVER (PARTITION BY match(Account, 'D3' ) order by Account) AS total_count_order
FROM
    card_data.large_patterns
WHERE
    match(Account, 'D3')
limit 10;




SELECT
    Account,
    LENGTH(Account) AS Account_Length
FROM card_data.large_patterns
WHERE match(Account, '^.{16,}$')
LIMIT 100;




