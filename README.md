# Fast processing with Apache Arrow Example

This repo contains examples of how to use Apache Arrow DataFusion and Polars to process data in Rust.

## Content

- `datafusion_examples` does contain examples on reading and processing parquet with DataFusion
- `polars_examples` does contain examples on readging and processing parquet with Polars
- `deltalake_examples` does contain examples on accessing Delta Lake tables
- `data` contains the needed data for examples to run

## Getting the source data

### Parquet

The two parquet datasets used in `datafusion_examples` and `polars_examples` projects has to be retrieved from OpenDataBlend ([opendatablend.io](https://www.opendatablend.io)).

#### Steps

Go to [Datasets](https://www.opendatablend.io/datasets) and under **Transport** section select [Anonymised MOT](https://www.opendatablend.io/dataset?name=open-data-blend-anonymised-mot). 

Scroll to the **Vehicle** dataset and click **Download** / **.parquet**. This will download a file named like `data-dimension-vehicle-*-vehicle.parquet`. Move it in `data/parquet` forlder and rename it to `data-dimension-vehicle.parquet`.

Scroll to the **Anonymised MOT Test Items 2021** dataset and click **Download** / **.parquet**. This will download a file named like `data-fact-anonymised_mot_test_item-2021-*-anonymised_mot_test_item_2021.parquet`. Move it in `data/parquet` forlder and rename it to `data-fact-anonymised_mot_test_item-2021.parquet`.

#### Direct download links (may not work)

[`data-dimension-vehicle-0220719T072336Z-vehicle.parquet`](https://odsasadatapackagesprod.blob.core.windows.net/packages/data%2Fdimension%2Fvehicle%2F20220719T072336Z%2Fvehicle.parquet?sv=2020-02-10&se=2023-02-27T13%3A20%3A43Z&sr=b&sp=rw&sig=apLdd0DCgN8YS71dSmiX2tEX6gB9ZWQFr9CMUZvTNHc%3D)

[`data-fact-anonymised_mot_test_item-2021-20220719T091833Z-anonymised_mot_test_item_2021.parquet`](https://odsasadatapackagesprod.blob.core.windows.net/packages/data%2Ffact%2Fanonymised_mot_test_item%2F2021%2F20220719T091833Z%2Fanonymised_mot_test_item_2021.parquet?sv=2020-02-10&se=2023-02-27T13%3A16%3A23Z&sr=b&sp=rw&sig=QuGLIOlpALEWgH4dVoPijDWr1y8VPFukWXHGar1leGI%3D)

### Delta Lake

The Delta Lake table used in the `delta_lake_examples` is compressed inside the `delta_tables/data-dimension-vehicle.zip` file.

Unzip with:

```bash
cd delta_tables/
tar -zxvf data-dimension-vehicle.zip && rm ./data-dimension-vehicle.zip
```

## Running

Do `cd` in either `datafusion_examples`, `delta_lake_examples` or `polars_examples` and run:

```bash
cargo run
```