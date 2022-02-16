# Databricks notebook source

from databricks_processing.scd_modelling.jobs import chunk_based_job

chunk_based_job(
    spark,
    dbutils,
    table_path="/mnt/dpdatastg/raw/ecoportal/dbo/client/",
    chunk_partition_number=20,
    pk="idClient",
    metastore_name="silver_ecoportal.dbo_client",
    storage_path="/mnt/dpdatastg/silver/ecoportal/dbo/client",
    mode="overwrite",
    log_file="client.log",
    log_path="/dbfs/mnt/dpdatastg/silver/logs/ecoportal/dbo/",
)
