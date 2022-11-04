package be.icteam.iceberg

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    /*
    git clone https://github.com/timvw/arrow-testing.git

    docker run \
    --rm \
    --publish 9000:9000 \
    --publish 9001:9001 \
    --name minio \
    --volume "/Users/timvw/src/github/qv/testing:/data" \
    --env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
    --env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
    quay.io/minio/minio:RELEASE.2022-05-26T05-48-41Z server /data \
    --console-address ":9001"
     */

   // https://iceberg.apache.org/docs/latest/spark-configuration/
    //https://spark.apache.org/docs/3.0.3/cloud-integration.html#installation

    implicit val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", "s3://data/iceberg")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://localhost:9000")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access","True")

    spark.sql("""CREATE DATABASE IF NOT EXISTS spark_catalog.db""")
    spark.sql("SHOW catalogs").show(10, false)
    spark.sql("SHOW tables").show(10, false)

    // read some existing sample data
    val df = spark.read.parquet("s3://data/delta/COVID-19_NYT/part-00000-a496f40c-e091-413a-85f9-b1b69d4b3b4e-c000.snappy.parquet")


    // (re-)create empty table in spark_catalog.db
    val covidTable = "spark_catalog.db.`COVID-19_NYT`"
    df.limit(0).writeTo(covidTable).using("iceberg").createOrReplace()

    // write data into table
    df.writeTo(covidTable).append()
    spark.sql(s"""SELECT * FROM $covidTable""").show(10, false)

    // write more data into table
    val df2 = spark.read.parquet("s3://data/delta/COVID-19_NYT/*.parquet")
    df2.writeTo(covidTable).using("iceberg").replace()

    spark.sql(s"select * from $covidTable").show(10, false)

    spark.stop()
  }
}