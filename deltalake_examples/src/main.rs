#![allow(warnings, unused)]

use std::{time::Instant, sync::Arc};

use deltalake::{open_table, datafusion::{prelude::SessionContext}, DeltaTable};
use deltalake::datafusion::{error::Result};

#[tokio::main]
async fn main() -> Result<()> {
    
    let table_uri = "../data/delta_tables/data-dimension-vehicle";
    let delta_table = open_table(table_uri).await.unwrap();
    let ctx = SessionContext::new();

    let start = Instant::now();

    query1_agg(ctx, delta_table).await.unwrap();
    
    println!("Elapsed: {:?}ms", start.elapsed().as_millis());
    Ok(())
}

async fn query1_agg(ctx: SessionContext, delta_table: DeltaTable) -> Result<()> {

    ctx.register_table("drv1_table", Arc::new(delta_table)).unwrap();
    let df = ctx
        .sql(
            
            "SELECT \
               MIN(drv_cylinder_capacity_integer) AS min, \
               MAX(drv_cylinder_capacity_integer) AS max, \
               AVG(drv_cylinder_capacity_integer) AS avg \
             FROM drv1_table"

        ).await.unwrap();
    df.show().await

}

