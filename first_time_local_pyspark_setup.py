from sedona.spark import SedonaContext


config = (
    SedonaContext
    .builder()
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.sedona:sedona-spark-3.5_2.12:1.6.0",
            "org.datasyslab:geotools-wrapper:1.6.0-28.2",
            "com.databricks:spark-xml_2.12:0.18.0",
        ]))
    .getOrCreate()
)

sedona = SedonaContext.create(config)
