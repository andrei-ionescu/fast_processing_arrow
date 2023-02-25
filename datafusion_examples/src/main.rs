#![allow(warnings, unused)]

use std::time::Instant;

use datafusion::prelude::*;
use datafusion::{error::Result};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let filename1 = "../data/parquet/data-fact-anonymised_mot_test_item-2021-20220620T090029Z-anonymised_mot_test_item_2021.parquet";
    let filename2 = "../data/parquet/data-dimension-vehicle-20220620T085710Z-vehicle.parquet";
    let start = Instant::now();

    // query1(ctx, filename1).await.unwrap();
    // query1_sql(ctx, filename1).await.unwrap();

    // query2(ctx, filename1).await.unwrap();
    query2_sql(ctx, filename1).await.unwrap();

    // query3(ctx, filename1, filename2).await.unwrap();
    // query3_sql(ctx, filename1, filename2).await.unwrap();

    println!("Elapsed: {:?}ms", start.elapsed().as_millis());
    Ok(())
}

async fn query1(ctx: SessionContext, filename: &str) -> Result<()> {

    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default()).await.unwrap()
        .select_columns(&["drv_vehicle_key", "drv_vehicle_first_use_date_key", "drv_mileage_key"]).unwrap()
        .filter(col("drv_vehicle_first_use_date_key").gt(lit(20090825))).unwrap()
        .limit(0, Some(10)).unwrap();
    df.show().await

}

async fn query1_sql(ctx: SessionContext, filename: &str) -> Result<()> {

    ctx.register_parquet("drv1_table", filename, ParquetReadOptions::default()).await.unwrap(); 
    let df = ctx
        .sql(

            "SELECT drv_vehicle_key, drv_vehicle_first_use_date_key, drv_mileage_key \
             FROM drv1_table \
             WHERE drv_vehicle_first_use_date_key > 20090825 \
             LIMIT 10"

        ).await.unwrap();
    df.show().await

}

async fn query2(ctx: SessionContext, filename: &str) -> Result<()> {

    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default()).await.unwrap()
        .aggregate(vec![], 
            vec![
                min(col("drv_mileage_key")).alias("min"), 
                avg(col("drv_mileage_key")).alias("avg"), 
                max(col("drv_mileage_key")).alias("max")
            ]
        ).unwrap();
    df.show().await

}

async fn query2_sql(ctx: SessionContext, filename: &str) -> Result<()> {

    ctx.register_parquet("drv1_table", filename, ParquetReadOptions::default()).await.unwrap(); 
    let df = ctx
        .sql(

            "SELECT MIN(drv_mileage_key) AS min, AVG(drv_mileage_key) AS avg, MAX(drv_mileage_key) AS max \
             FROM drv1_table"

        ).await.unwrap();
    df.show().await

}

async fn query3(ctx: SessionContext, filename1: &str, filename2: &str) -> Result<()> {

    let df1 = ctx
        .read_parquet(filename1, ParquetReadOptions::default()).await.unwrap()
        .select(vec![
            col("drv_vehicle_key"),
            col("drv_mileage_key"),
            col("drv_vehicle_first_use_date_key"),
        ]).unwrap()
        .limit(0, Some(10000)).unwrap()
        .filter(col("drv_vehicle_key").gt(lit(0))).unwrap();
    let df2 = ctx
        .read_parquet(filename2, ParquetReadOptions::default()).await.unwrap()
        .select(vec![
            col("drv_vehicle_key").alias("drv_vehicle_key2"),
            col("src_vehicle_make"),
            col("src_vehicle_model"),
        ]).unwrap()
        .limit(0, Some(10000)).unwrap()
        .filter(col("drv_vehicle_key").gt(lit(0))).unwrap();
    let joined = df1
        .join(
            df2, 
            JoinType::Inner, 
            &["drv_vehicle_key"], &["drv_vehicle_key2"], None
        ).unwrap();
    joined.show().await

}

async fn query3_sql(ctx: SessionContext, filename1: &str, filename2: &str) -> Result<()> {

    ctx.register_parquet("drv1_table", filename1, ParquetReadOptions::default()).await.unwrap(); 
    ctx.register_parquet("drv2_table", filename2, ParquetReadOptions::default()).await.unwrap(); 

    let joined = ctx
        .sql(

            "SELECT \
                a.drv_vehicle_key, \
                a.drv_mileage_key, \
                a.drv_vehicle_first_use_date_key, \
                b.src_vehicle_make, \
                b.src_vehicle_model \
             FROM ( \
                SELECT \
                    drv_vehicle_key, \
                    drv_mileage_key, \
                    drv_vehicle_first_use_date_key \
                FROM drv1_table \
                WHERE drv_vehicle_key > 0 \
                LIMIT 10000 \
             ) AS a \
             INNER JOIN ( \
                SELECT \
                    drv_vehicle_key, \
                    src_vehicle_make, \
                    src_vehicle_model \
                FROM drv2_table \
                WHERE drv_vehicle_key > 0 \
                LIMIT 10000) AS b \
             ON a.drv_vehicle_key = b.drv_vehicle_key"

        ).await.unwrap();
    joined.show().await

}
