-- Active: 1742332639624@@104.196.195.168@8123@card_data
Create DATABASE IF NOT EXISTS card_data;


create table card_data.large_patterns (
            Timestamp String
            ,`From Bank` Int64
            ,Account String
            ,`To Bank` Nullable(Float32)
            ,Account_1 Nullable(String)
            ,`Amount Received` Nullable(Float64)
            ,`Receiving Currency` Nullable(String)
            ,`Amount Paid` Nullable(Float64)
            ,`Payment Currency` Nullable(String)
            ,`Payment Format` Nullable(String)
            ,`Is Laundering` Nullable(Float64)
            ,`Anomaly Type` Nullable(String)
) ENGINE = MergeTree()
    ORDER BY Timestamp

select * from card_data.large_patterns
LIMIT 100;


select * from card_data.medium_transactions
LIMIT 100;

SHOW TABLES FROM card_data;