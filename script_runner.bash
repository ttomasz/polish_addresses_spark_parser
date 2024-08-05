#!bin/bash

set -e

rm -f data_zip/*.zip
rm -f data_xml/*.xml
python3 download_and_unpack.py
rm data_zip/*.zip
python3 process_gml.py osmpoland
cp data_geoparquet/files/part*.parquet data_export/prg_adresy.parquet
chmod 666 data_export/prg_adresy.parquet
python3 process_gml.py overture
cp data_geoparquet/files/part*.parquet data_export/poland_addresses.parquet
chmod 666 data_export/poland_addresses.parquet
rm data_xml/*.xml
rm data_geoparquet/files/*.parquet
rm data_geoparquet/files/.*.crc
rm data_geoparquet/files/_*
