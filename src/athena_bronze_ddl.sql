CREATE DATABASE IF NOT EXISTS `default`;

-- ============================================================
-- Hudi MOR tables for Athena (Snapshot View)
-- Uses HoodieParquetRealtimeInputFormat to read both
-- compacted base files AND delta log files for full accuracy.
-- ============================================================

-- Table: actor
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`actor` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `actor_id` INT,
  `first_name` STRING,
  `last_name` STRING,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/actor/'
TBLPROPERTIES ('classification'='hudi');

-- Table: address
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`address` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `address_id` INT,
  `address` STRING,
  `address2` STRING,
  `district` STRING,
  `city_id` INT,
  `postal_code` STRING,
  `phone` STRING,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/address/'
TBLPROPERTIES ('classification'='hudi');

-- Table: category
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`category` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `category_id` INT,
  `name` STRING,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/category/'
TBLPROPERTIES ('classification'='hudi');

-- Table: city
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`city` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `city_id` INT,
  `city` STRING,
  `country_id` INT,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/city/'
TBLPROPERTIES ('classification'='hudi');

-- Table: country
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`country` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `country_id` INT,
  `country` STRING,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/country/'
TBLPROPERTIES ('classification'='hudi');

-- Table: customer
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`customer` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `customer_id` INT,
  `store_id` INT,
  `first_name` STRING,
  `last_name` STRING,
  `email` STRING,
  `address_id` INT,
  `activebool` BOOLEAN,
  `create_date` STRING,
  `last_update` STRING,
  `active` INT,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/customer/'
TBLPROPERTIES ('classification'='hudi');

-- Table: film
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`film` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `film_id` INT,
  `title` STRING,
  `description` STRING,
  `release_year` INT,
  `language_id` INT,
  `rental_duration` INT,
  `rental_rate` STRING,
  `length` INT,
  `replacement_cost` STRING,
  `rating` STRING,
  `last_update` STRING,
  `special_features` ARRAY<STRING>,
  `fulltext` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/film/'
TBLPROPERTIES ('classification'='hudi');

-- Table: film_actor
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`film_actor` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `actor_id` INT,
  `film_id` INT,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/film_actor/'
TBLPROPERTIES ('classification'='hudi');

-- Table: film_category
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`film_category` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `film_id` INT,
  `category_id` INT,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/film_category/'
TBLPROPERTIES ('classification'='hudi');

-- Table: inventory
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`inventory` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `inventory_id` INT,
  `film_id` INT,
  `store_id` INT,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/inventory/'
TBLPROPERTIES ('classification'='hudi');

-- Table: language
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`language` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `language_id` INT,
  `name` STRING,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/language/'
TBLPROPERTIES ('classification'='hudi');

-- Table: payment
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`payment` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `payment_id` INT,
  `customer_id` INT,
  `staff_id` INT,
  `rental_id` INT,
  `amount` STRING,
  `payment_date` STRING,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/payment/'
TBLPROPERTIES ('classification'='hudi');

-- Table: rental
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`rental` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `rental_id` INT,
  `rental_date` STRING,
  `inventory_id` INT,
  `customer_id` INT,
  `return_date` STRING,
  `staff_id` INT,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/rental/'
TBLPROPERTIES ('classification'='hudi');

-- Table: staff
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`staff` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `staff_id` INT,
  `first_name` STRING,
  `last_name` STRING,
  `address_id` INT,
  `email` STRING,
  `store_id` INT,
  `active` BOOLEAN,
  `username` STRING,
  `password` STRING,
  `last_update` STRING,
  `picture` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/staff/'
TBLPROPERTIES ('classification'='hudi');

-- Table: store
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`store` (
  `_hoodie_commit_time` STRING,
  `_hoodie_commit_seqno` STRING,
  `_hoodie_record_key` STRING,
  `_hoodie_partition_path` STRING,
  `_hoodie_file_name` STRING,
  `store_id` INT,
  `manager_staff_id` INT,
  `address_id` INT,
  `last_update` STRING,
  `__ts_ms` BIGINT,
  `__op` STRING,
  `__deleted` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://dvd-rental-lakehouse/bronze/default/store/'
TBLPROPERTIES ('classification'='hudi');

-- ============================================================
-- Verification query: compare row counts across all tables
-- ============================================================
-- SELECT 'actor' as tbl, COUNT(*) as cnt FROM default.actor WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'address', COUNT(*) FROM default.address WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'category', COUNT(*) FROM default.category WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'city', COUNT(*) FROM default.city WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'country', COUNT(*) FROM default.country WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'customer', COUNT(*) FROM default.customer WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'film', COUNT(*) FROM default.film WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'film_actor', COUNT(*) FROM default.film_actor WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'film_category', COUNT(*) FROM default.film_category WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'inventory', COUNT(*) FROM default.inventory WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'language', COUNT(*) FROM default.language WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'payment', COUNT(*) FROM default.payment WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'rental', COUNT(*) FROM default.rental WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'staff', COUNT(*) FROM default.staff WHERE __deleted IS NULL OR __deleted != 'true'
-- UNION ALL SELECT 'store', COUNT(*) FROM default.store WHERE __deleted IS NULL OR __deleted != 'true';
