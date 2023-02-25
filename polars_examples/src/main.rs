#![allow(warnings, unused)]

use std::time::Instant;

use polars::prelude::*;
use polars::lazy::dsl::*;
use polars_sql::SQLContext;
// use polars_sql::*;

fn main() {
    let filename1 = "../data/parquet/data-fact-anonymised_mot_test_item-2021-20220620T090029Z-anonymised_mot_test_item_2021.parquet";
    let filename2 = "../data/parquet/data-dimension-vehicle-20220620T085710Z-vehicle.parquet";
    let start = Instant::now();

    // query1(filename1);
    // query1_sql(filename1);

    // query2(filename1);
    // query2_sql(filename1);

    // query3(filename1, filename2);
    query3_sql(filename1, filename2);

    println!("Elapsed: {:?}ms", start.elapsed().as_millis());
}

fn query1(filename: &str) {

    let pq_df = LazyFrame::scan_parquet(filename, Default::default()).unwrap();
    let df = pq_df
        .filter(col("drv_vehicle_first_use_date_key").gt(lit(20090825)))
        .select(vec![
            col("drv_vehicle_key"), 
            col("drv_vehicle_first_use_date_key"), 
            col("drv_mileage_key"),
        ])
        .limit(10).collect().unwrap();
    dbg!(df);

}

fn query1_sql(filename: &str) {

    let mut sql_context = SQLContext::try_new().unwrap();
    let pq_df = LazyFrame::scan_parquet(filename, Default::default()).unwrap();
    sql_context.register("drv1_table", pq_df);

    let df = sql_context
        .execute(

            "SELECT drv_vehicle_key, drv_vehicle_first_use_date_key, drv_mileage_key \
             FROM drv1_table \
             WHERE drv_vehicle_first_use_date_key > 20090825 \
             LIMIT 10"

        ).unwrap()
        .collect().unwrap();
    dbg!(df);

}

fn query2(filename: &str) {

    let pq_df = LazyFrame::scan_parquet(filename, Default::default()).unwrap();
    let df = pq_df.select([
        min("drv_mileage_key").alias("min"),
        avg("drv_mileage_key").alias("avg"),
        max("drv_mileage_key").alias("max")
    ]).collect().unwrap();
    dbg!(df);

}

fn query2_sql(filename: &str) {

    let mut sql_context = SQLContext::try_new().unwrap();
    let pq_df = LazyFrame::scan_parquet(filename, Default::default()).unwrap();
    sql_context.register("drv1_table", pq_df);

    let df = sql_context
        .execute(

            "SELECT MIN(drv_mileage_key) AS min, AVG(drv_mileage_key) AS avg, MAX(drv_mileage_key) AS max \
             FROM drv1_table"

        ).unwrap()
        .collect().unwrap();
    dbg!(df);

}

fn query3(filename1: &str, filename2: &str) {

    let pq_df1 = LazyFrame::scan_parquet(filename1, Default::default()).unwrap()
        .select([
            col("drv_vehicle_key"),
            col("drv_mileage_key"),
            col("drv_vehicle_first_use_date_key"),
        ])
        .limit(10000)
        .filter(col("drv_vehicle_key").gt(lit(0)));
    let pq_df2 = LazyFrame::scan_parquet(filename2, Default::default()).unwrap()
        .select([
            col("drv_vehicle_key"),
            col("src_vehicle_make"),
            col("src_vehicle_model"),
        ])
        .limit(10000)
        .filter(col("drv_vehicle_key").gt(lit(0)));
    let df = pq_df1
        .join(
            pq_df2, 
            [col("drv_vehicle_key")], 
            [col("drv_vehicle_key")], 
            JoinType::Inner)
        .select([
            col("drv_vehicle_key"), 
            col("drv_mileage_key"), 
            col("drv_vehicle_first_use_date_key"),
            col("src_vehicle_make"),
            col("src_vehicle_model"),
        ])
        .collect().unwrap();
    dbg!(df);

}

fn query3_sql(filename1: &str, filename2: &str) {

    let mut sql_context = SQLContext::try_new().unwrap();
    let pq_df1 = LazyFrame::scan_parquet(filename1, Default::default()).unwrap();
    let pq_df2 = LazyFrame::scan_parquet(filename2, Default::default()).unwrap();
    sql_context.register("drv1_table", pq_df1);
    sql_context.register("drv2_table", pq_df2);

    let df = sql_context
        .execute(

            "SELECT \
                drv_vehicle_key, \
                drv_mileage_key, \
                drv_vehicle_first_use_date_key, \
                src_vehicle_make, \
                src_vehicle_model \
            FROM drv1_table \
            LEFT JOIN drv2_table \
            ON drv1_table.drv_vehicle_key = drv2_table.drv_vehicle_key 
            WHERE drv_vehicle_key > 0 \
            LIMIT 10000"

        ).unwrap()
        .collect().unwrap();
    dbg!(df);

}