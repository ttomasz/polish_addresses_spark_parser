import os
from pathlib import Path
import zipfile

import requests


GML_URL = "https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml"


this_file = Path(__file__)
this_dir = this_file.parent
zip_file_path = this_dir / "data_zip" / "prg.zip"
xml_dir = this_dir / "data_xml"


def download_file(url: str, to: Path) -> None:
    print("Downloading data from:", url, "to file:", to)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(to, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    print("Finished downloading.")


def unzip_files(zip_file_path: Path, to: Path) -> None:
    print("Unpacking zip file:", zip_file_path)
    with zipfile.ZipFile(zip_file_path, "r") as zip:
        for zf in zip.filelist:
            new_name = zf.filename.split("_")[-1:][0]
            print(f"Extracting {zf.filename} to {new_name}")
            zip.extract(zf, xml_dir)
            os.rename(xml_dir / zf.filename, xml_dir / new_name)
    print("Finished unpacking zip file.")


if __name__ == "__main__":
    download_file(url=GML_URL, to=zip_file_path)
    unzip_files(zip_file_path, xml_dir)
