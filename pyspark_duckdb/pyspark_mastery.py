from pandas.core.window import Window
from math import trunc
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession
import os
from pyspark.sql.window import Window

def create_spark_session(app_name="PySpark"):
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.client.region", "ap-southeast-2") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("APP_NAME: ", app_name)

    return spark

if __name__ == "__main__":
    spark = create_spark_session()

    catalog_database = "glue_catalog.dvdrental_mart"

    table_name = "fct_payment"

    df_payment:DataFrame = spark.table(f"{catalog_database}.{table_name}")

    df_customer:DataFrame = spark.table(f"{catalog_database}.dim_customer")

    df_store:DataFrame = spark.table(f"{catalog_database}.dim_store")

    df_rental:DataFrame = spark.table(f"{catalog_database}.fct_rental")

    df_film:DataFrame = spark.table(f"{catalog_database}.dim_film")

    
    """ 
    df.printSchema()

    df.show(10)

    df.count()

    print(df.count())

    df.describe(["rental_rate", "replacement_cost"]).show() """

    """ 
    - From dim_film: Select only film_id, title, rating, rental_rate
    - Filter films with rating = 'R' AND rental_rate > 2.99
    - Filter films where title CONTAINS the word 'LOVE' (use F.col().contains())
    - Sort by rental_rate descending, then by title ascending
    """ 
    """
            
        df.select(F.col("film_id"), F.col("title"), F.col("rating"), F.col("rental_rate")).show(50)

        df.filter((F.col("rating") == "R" ) & (F.col("rental_rate") > 2.99)).show(50)

        df.filter(F.col("title").contains("LOVE")).show(50)

        df.sort(F.col("rental_rate").desc()).show(50)
    """

    """ 
    - From fct_payment: Create a column "payment_category" using F.when():
    - amount < 3.00  → "Low"
    - amount < 6.00  → "Medium"
    - amount >= 6.00 → "High"
    - From dim_customer: Create "full_name" by concatenating first_name + ' ' + last_name
    - From fct_rental: Calculate rental_duration_days (return_date - rental_date) 
    using F.datediff()
    """

    """ 
        df_payment.printSchema()

        df_payment_category = df_payment.withColumn("payment_catefory",
        F.when(F.col("amount") < 3.00, "Low")
            .when(F.col("amount") < 6.00, "Medium")
            .otherwise("High"))
        
        df_payment_category.show(50)

        df_customer_fullname = df_customer.select(F.col("customer_id"), F.col("first_name"), F.col("last_name")).withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
        
        df_customer_fullname.show(50)
    """
    """ 
        - Total revenue per store (fct_payment grouped by store_id)
       - Average rental duration per film rating (join fct_rental with dim_film)
        - Count of rentals per month (extract month from rental_date using F.month())
       - Find the TOP 10 customers by total spending
    """

    """
    print("Total revenue per store")

    df_temp = df_payment.join(F.broadcast(df_store), on=["store_id"], how="left")

    df_total_Revenue_per_store = df_temp.groupBy("store_id").agg(
        F.sum(F.coalesce(F.col("amount"), F.lit(0))).alias("total_revenue")
    )

    print("Average rental duration per film rating (join fct_rental with dim_film")

    df_temp2 = df_film.join(df_rental, on=["film_id"], how="left")

    df_avg_rental_duration_per_film = df_temp2.groupBy("rating").agg(
        F.avg("rental_duration").alias("Avg rental duration")
    )

    print("Count of rentals per month (extract month from rental_date using F.month")

    df_temp3 = df_rental.withColumn("rental_month", F.date_format(F.col("rental_date"), "yyyy-MM"))

    df_rental_per_monthh = df_temp3.groupBy("rental_month").agg(
        F.count(F.coalesce(F.col("rental_id"), F.lit(0))).alias("total_rental")
    )

    print("Find the TOP 10 customers by total spending")

    df_fact_agg = df_payment.groupBy("customer_id").agg(
        F.sum(F.coalesce(F.col("amount"), F.lit(0))).alias("total_spend")
    ).orderBy(F.col("total_spend").desc()).limit(10)

    df_final = df_customer.join(df_fact_agg, on=["customer_id"], how="inner")

    df_final.show()

    """

    """     Build a "rental enriched" DataFrame by joining:
        fct_rental + dim_customer (on customer_id)  
                    + dim_film (on film_id)
                    + dim_store (on store_id)

        Final columns: rental_id, rental_date, customer full_name, 
                    film title, film category_name, store_id

        - Use LEFT joins
        - Remember: pass join keys as a list to avoid duplicate columns!
    """
    """ 
    df_rental_enriched = df_rental.join(df_customer, on=["customer_id"], how="left").join(df_film, on=["film_id"], how="left").join(df_store, on=["store_id"], how="left")

    df_rental_enriched.show(100)

     """

    """     df_rental_join = df_rental.join(F.broadcast(df_store), on=["store_id"], how="left").explain()

        df_rental_broadcast_join = df_rental.join(df_store, on=["store_id"], how="left").explain()
    """

    """ 
    - Calculate total_spent per customer per store from fct_payment
    - Use a Window Function to RANK customers within each store 
    (PARTITION BY store_id ORDER BY total_spent DESC)
    - Filter to show only the Top 5 customers per store
    """

    """     df_temp = df_payment.groupBy(["customer_id", "store_id"]).agg(
        F.sum(F.coalesce(F.col("amount"), F.lit(0))).alias("total_spend_per_customer_per_store")
    )

    window_spec = Window.partitionBy("store_id").orderBy(F.col("total_spend_per_customer_per_store").desc())

    df_rank = df_temp.withColumn("rank", F.row_number().over(window_spec))

    df_rank.show(50)

    df_final = df_rank.filter(F.col("rank") <= 5)

    df_final.show(50) """

    """
    Challenge 8: Running Totals & Moving Averages
    Goal: Cumulative calculations.


    - From fct_daily_revenue: Calculate cumulative total_revenue per store 
    ordered by payment_date_day
    - Calculate a 7-day moving average of total_revenue per store
    - Use Window with rowsBetween() for the moving average
    Key Syntax: Window.partitionBy().orderBy().rowsBetween(), F.sum().over(), F.avg().over()

    """
        
    """     window_spec = Window.partitionBy("store_id").orderBy(F.col("payment_date_day").asc())

        window_spec_7_duration = Window.partitionBy("store_id").orderBy(F.col("payment_date_day")).rowsBetween(-6, Window.currentRow)

        df_revenue_per_day = df_payment.groupBy(["payment_date_day", "store_id"]).agg(
            F.sum(F.coalesce(F.col("amount"), F.lit(0))).alias("total_revenue")
        )

        df_sum_cum = df_revenue_per_day.withColumn("cumulative_revenue", F.sum(F.col("total_revenue")).over(window_spec))
        
        df_sum_cum.show(50)

        df_avg_revenue = df_revenue_per_day.withColumn("avg_7_duration_revenue", F.avg(F.col("total_revenue")).over(window_spec_7_duration))

        df_avg_revenue.show(50)
    """

    
    """

    Challenge 9: Lag/Lead Analysis
    Goal: Compare current row with previous/next rows.


    - For each customer, find their previous rental date using F.lag()
    - Calculate "days_since_last_rental" (current rental_date - previous rental_date)
    - Flag customers with gaps > 30 days as "at_risk_churn"
    - Find which customer has the longest gap between two consecutive rentals
    Key Syntax: F.lag(), F.lead(), F.datediff()
    """

    windown_spec = Window.partitionBy("customer_id").orderBy(F.col("rental_date").asc())

    df_lastest_rental_per_customer = df_rental.groupBy("customer_id").agg(
        F.max(F.col("rental_date")).alias("previous_rental_date")
    )

    df_lastest_rental = df_rental.withColumn("previous_rental_date", F.lag("rental_date", 1).over(windown_spec).cast("date")).withColumn("rental_date_cast", F.col("rental_date").cast("date"))
    

    df_gaps = df_lastest_rental.withColumn("gaps", F.date_diff(F.col("rental_date_cast"), F.col("previous_rental_date")))

    df_gaps.show()

    df_highlight = df_gaps.withColumn("at_risk_churn", F.when(F.col("gaps") > 30, True).otherwise(False))

    df_final = df_highlight.filter(F.col("at_risk_churn") == True)

    df_final.show()
    





