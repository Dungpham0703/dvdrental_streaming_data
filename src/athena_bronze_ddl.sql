CREATE DATABASE IF NOT EXISTS `dvdrental_bronze`;

-- Table: actor
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`actor` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `actor_id` int,
  `first_name` string,
  `last_name` string,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/actor/';

-- Table: address
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`address` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `address_id` int,
  `address` string,
  `address2` string,
  `district` string,
  `city_id` int,
  `postal_code` string,
  `phone` string,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/address/';

-- Table: category
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`category` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `category_id` int,
  `name` string,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/category/';

-- Table: city
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`city` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `city_id` int,
  `city` string,
  `country_id` int,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/city/';

-- Table: country
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`country` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `country_id` int,
  `country` string,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/country/';

-- Table: customer
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`customer` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `customer_id` int,
  `store_id` int,
  `first_name` string,
  `last_name` string,
  `email` string,
  `address_id` int,
  `activebool` boolean,
  `create_date` string,
  `last_update` string,
  `active` int,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/customer/';

-- Table: film
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`film` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `film_id` int,
  `title` string,
  `description` string,
  `release_year` int,
  `language_id` int,
  `rental_duration` int,
  `rental_rate` string,
  `length` int,
  `replacement_cost` string,
  `rating` string,
  `last_update` string,
  `special_features` array<string>,
  `fulltext` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/film/';

-- Table: film_actor
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`film_actor` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `actor_id` int,
  `film_id` int,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/film_actor/';

-- Table: film_category
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`film_category` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `film_id` int,
  `category_id` int,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/film_category/';

-- Table: inventory
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`inventory` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `inventory_id` int,
  `film_id` int,
  `store_id` int,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/inventory/';

-- Table: language
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`language` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `language_id` int,
  `name` string,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/language/';

-- Table: payment
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`payment` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `payment_id` int,
  `customer_id` int,
  `staff_id` int,
  `rental_id` int,
  `amount` string,
  `payment_date` string,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/payment/';

-- Table: rental
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`rental` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `rental_id` int,
  `rental_date` string,
  `inventory_id` int,
  `customer_id` int,
  `return_date` string,
  `staff_id` int,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/rental/';

-- Table: staff
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`staff` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `staff_id` int,
  `first_name` string,
  `last_name` string,
  `address_id` int,
  `email` string,
  `store_id` int,
  `active` boolean,
  `username` string,
  `password` string,
  `last_update` string,
  `picture` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/staff/';

-- Table: store
CREATE EXTERNAL TABLE IF NOT EXISTS `dvdrental_bronze`.`store` (
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `store_id` int,
  `manager_staff_id` int,
  `address_id` int,
  `last_update` string,
  `__ts_ms` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dvd-rental-lakehouse/bronze/store/';
