import json
from pathlib import Path
from typing import List, Union
from sys import argv

from sedona.spark import SedonaContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, DateType
from pyspark.sql import functions as f


this_file = Path(__file__)
this_dir = this_file.parent
xml_dir = this_dir / "data_xml"
geoparquet_dir = this_dir / "data_geoparquet" / "files"

MODE_OVERTURE = "overture"
MODE_OSMPOLAND = "osmpoland"
MODES = {MODE_OVERTURE, MODE_OSMPOLAND}

schema = StructType([
    StructField("gml:identifier", StringType()),  # gml id
    StructField("prg-ad:idIIP", StructType([
        StructField("bt:BT_Identyfikator", StructType([
            StructField("bt:lokalnyId", StringType()),  # unique object identifier
            StructField("bt:przestrzenNazw", StringType()),  # namespace in IIP (Spatial Information Infrastructure)
            StructField("bt:wersjaId", TimestampType()),  # version id (timestamp)
    ]))])),
    StructField("prg-ad:cyklZycia", StructType([
        StructField("bt:BT_CyklZyciaInfo", StructType([
            StructField("bt:poczatekWersjiObiektu", TimestampType()),  # object beginning
            StructField("bt:koniecWersjiObiektu", TimestampType()),  # object end
    ]))])),
    StructField("prg-ad:waznyOd", DateType()),  # valid since
    StructField("prg-ad:waznyDo", DateType()),  # valid to
    StructField("prg-ad:jednostkaAdmnistracyjna", ArrayType(StringType())),  # administrative unit
    StructField("prg-ad:miejscowosc", StringType()),  # city/village
    StructField("prg-ad:czescMiejscowosci", StringType()),  # part of city/village
    StructField("prg-ad:ulica", StringType()),  # street name
    StructField("prg-ad:numerPorzadkowy", StringType()),  # housenumber
    StructField("prg-ad:kodPocztowy", StringType()),  # postal code
    StructField("prg-ad:status", StringType()),  # status (existing, being built, planned)
    StructField("prg-ad:pozycja", StructType([
        StructField("gml:Point", StructType([
            StructField("gml:pos", StringType()),
        ])),
    ])),  # gml geometry
    StructField("prg-ad:komponent", ArrayType(StructType([
        StructField("_xlink:href", StringType()),
    ]))),  # links to other gml objects for administrative units, places, and streets
    StructField("prg-ad:obiektEMUiA", StructType([
        StructField("_xlink:href", StringType()),
    ])),  # identifier in municipality system
])


def get_sedona_context() -> SparkSession:
    config = (
        SedonaContext
        .builder()
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")  # to allow writing old timestamps which apparently is needed? 
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

    mem = sedona.conf.get("spark.driver.memory", default=None)
    if mem is not None and mem == "1g":
        sedona.conf.set("spark.driver.memory", "3g")  # give the driver a bit more memory than default

    return sedona


def read_xml(spark: SparkSession, path: Union[str, List[str]]) -> DataFrame:
    return (
        spark.read
        .format("com.databricks.spark.xml")
        .schema(schema)
        .option("mode", "FAILFAST")  # throw an error if any row can't be parsed
        .option("rootTag", "gml:FeatureCollection")
        # there's also administrative units (prg-ad:PRG_JednostkaAdministracyjnaNazwa), places (prg-ad:PRG_MiejscowoscNazwa), and streets (prg-ad:PRG_UlicaNazwa)
        # but they are not needed to get addresses
        .option("rowTag", "prg-ad:PRG_PunktAdresowy")
        .option("inferSchema", "false")  # we specify schema manually above
        .option("ignoreNamespace", "false")
        .option("attributePrefix", "_")
        .load(path)
    )


def remove_closed_objects(df: DataFrame) -> DataFrame:
    return(
        df
        .where(f.col("prg-ad:cyklZycia.bt:BT_CyklZyciaInfo.bt:koniecWersjiObiektu").isNull())
        .where(f.col("prg-ad:waznyDo").isNull())
    )  # in case any object is marked as closed


def remove_planned_addresses(df: DataFrame) -> DataFrame:
    return df.where(f.col("prg-ad:status") != f.lit("prognozowany"))


def remove_unneeded_ids(df: DataFrame) -> DataFrame:
    return df.drop(
        "gml:identifier",  # we're gonna use IIP identifier
        "prg-ad:komponent",  # we don't need to join to any other records
        "prg-ad:obiektEMUiA",
    )


def parse_point_geometry(df: DataFrame) -> DataFrame:
    # ST_GeomFromGML couldn't parse the geometry so we're doing that manually which is simple enough for points
    return (
        df
        .withColumn("geometry", f.expr("""
            ST_Transform(
                ST_SetSRID(
                    ST_FlipCoordinates(
                        ST_GeomFromWKT(
                            concat('POINT(', `prg-ad:pozycja`.`gml:Point`.`gml:pos`, ')')
                        )
                    ),
                    2180
                ),
                'EPSG:4326'
            )
        """))
        .drop("prg-ad:pozycja")
    )


def select_cols_for_overture(df: DataFrame) -> DataFrame:
    return df.select(
        f.col("prg-ad:idIIP.bt:BT_Identyfikator.bt:przestrzenNazw").alias("id_namespace"),
        f.col("prg-ad:idIIP.bt:BT_Identyfikator.bt:lokalnyId").alias("unique_id"),
        f.col("prg-ad:idIIP.bt:BT_Identyfikator.bt:wersjaId").alias("object_timestamp"),
        f.col("prg-ad:jednostkaAdmnistracyjna").alias("administrative_units"),
        f.col("prg-ad:miejscowosc").alias("place"),
        f.col("prg-ad:czescMiejscowosci").alias("place_part"),
        f.col("prg-ad:ulica").alias("street"),
        f.col("prg-ad:numerPorzadkowy").alias("housenumber"),
        f.col("prg-ad:kodPocztowy").alias("postal_code"),
        f.col("geometry"),
    )

def select_cols_for_osmpoland(df: DataFrame) -> DataFrame:
    return df.select(
        f.col("prg-ad:idIIP.bt:BT_Identyfikator.bt:przestrzenNazw").alias("przestrzenNazw"),
        f.col("prg-ad:idIIP.bt:BT_Identyfikator.bt:lokalnyId").alias("lokalnyId"),
        f.col("prg-ad:idIIP.bt:BT_Identyfikator.bt:wersjaId").alias("wersjaId"),
        *[f.col("prg-ad:jednostkaAdmnistracyjna")[i].alias(f"jednostkaAdmnistracyjna_{i}") for i in range(4)],
        f.col("prg-ad:miejscowosc").alias("miejscowosc"),
        f.col("prg-ad:czescMiejscowosci").alias("czescMiejscowosci"),
        f.col("prg-ad:ulica").alias("ulica"),
        f.col("prg-ad:numerPorzadkowy").alias("numerPorzadkowy"),
        f.col("prg-ad:kodPocztowy").alias("kodPocztowy"),
        f.col("prg-ad:status").alias("status"),
        f.col("geometry"),
        f.col("gml:identifier"),
        f.col("prg-ad:komponent"),
        f.col("prg-ad:obiektEMUiA"),
        f.col("prg-ad:cyklZycia.bt:BT_CyklZyciaInfo.bt:poczatekWersjiObiektu").alias("poczatekWersjiObiektu"),
        f.col("prg-ad:cyklZycia.bt:BT_CyklZyciaInfo.bt:koniecWersjiObiektu").alias("koniecWersjiObiektu"),
        f.col("prg-ad:waznyOd").alias("prg-ad:waznyOd"),
        f.col("prg-ad:waznyDo").alias("prg-ad:waznyDo"),
    )


if __name__ == "__main__":
    if len(argv) == 1:
        raise AttributeError(f"Need to provide execution parameter: {MODES}")
    mode = argv[1].strip().lower()
    if mode not in MODES:
        raise ValueError(f"mode: {mode} not one of: {MODES}")

    xml_paths = f"{xml_dir}/*.xml"
    print("Starting parsing xml files:", xml_paths)
    spark = get_sedona_context()

    df = read_xml(spark=spark, path=xml_paths)
    if mode == MODE_OVERTURE:
        df = remove_unneeded_ids(df=df)
        df = remove_closed_objects(df=df)
        df = remove_planned_addresses(df=df)
    df = parse_point_geometry(df=df)

    if mode == MODE_OVERTURE:
        df = select_cols_for_overture(df=df).cache()
    elif mode == MODE_OSMPOLAND:
        df = select_cols_for_osmpoland(df=df).cache()
    else:
        raise ValueError(f"Unknown mode: {mode}, should be one of: {MODES}")

    num_rows = df.count()
    print(f"Data has: {num_rows} rows.")
    if num_rows == 0:
        raise ValueError("No data to write.")

    output_path = geoparquet_dir.absolute().as_posix()
    print("Writing geoparquet files to:", output_path)
    # CRS84 definition - like WGS84 (epsg:4326) but with lon, lat order instead of lat, lon
    projjson = json.dumps(
        {
          "$schema": "https://proj.org/schemas/v0.6/projjson.schema.json",
          "area": "World.",
          "bbox": {
            "east_longitude": 180,
            "north_latitude": 90,
            "south_latitude": -90,
            "west_longitude": -180
          },
          "coordinate_system": {
            "axis": [
              {
                "abbreviation": "Lon",
                "direction": "east",
                "name": "Geodetic longitude",
                "unit": "degree"
              },
              {
                "abbreviation": "Lat",
                "direction": "north",
                "name": "Geodetic latitude",
                "unit": "degree"
              }
            ],
            "subtype": "ellipsoidal"
          },
          "datum_ensemble": {
            "accuracy": "2.0",
            "ellipsoid": {
              "inverse_flattening": 298.257223563,
              "name": "WGS 84",
              "semi_major_axis": 6378137
            },
            "id": {
              "authority": "EPSG",
              "code": 6326
            },
            "members": [
              {
                "id": {
                  "authority": "EPSG",
                  "code": 1166
                },
                "name": "World Geodetic System 1984 (Transit)"
              },
              {
                "id": {
                  "authority": "EPSG",
                  "code": 1152
                },
                "name": "World Geodetic System 1984 (G730)"
              },
              {
                "id": {
                  "authority": "EPSG",
                  "code": 1153
                },
                "name": "World Geodetic System 1984 (G873)"
              },
              {
                "id": {
                  "authority": "EPSG",
                  "code": 1154
                },
                "name": "World Geodetic System 1984 (G1150)"
              },
              {
                "id": {
                  "authority": "EPSG",
                  "code": 1155
                },
                "name": "World Geodetic System 1984 (G1674)"
              },
              {
                "id": {
                  "authority": "EPSG",
                  "code": 1156
                },
                "name": "World Geodetic System 1984 (G1762)"
              },
              {
                "id": {
                  "authority": "EPSG",
                  "code": 1309
                },
                "name": "World Geodetic System 1984 (G2139)"
              }
            ],
            "name": "World Geodetic System 1984 ensemble"
          },
          "id": {
            "authority": "OGC",
            "code": "CRS84"
          },
          "name": "WGS 84 (CRS84)",
          "scope": "Not known.",
          "type": "GeographicCRS"
        }
    )
    # write geoparquet
    (
        df
        .coalesce(1)  # we want 1 result file
        .write
        .format("geoparquet")
        .option("geoparquet.version", "1.0.0")
        .option("geoparquet.crs", projjson)
        .save(path=output_path, mode="overwrite", compression="zstd")
    )
    print("Finished writing geoparquet files.")
