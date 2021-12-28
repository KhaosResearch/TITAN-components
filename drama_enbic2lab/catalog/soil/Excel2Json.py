import os
import zipfile
from pathlib import Path
import pandas as pd

from drama.core.model import TempFile
from drama.process import Process
from drama.models.task import TaskResult


def execute(pcs: Process, author: str = None, group: str = None, project: str = None):
    # read inputs
    inputs = pcs.get_from_upstream()

    input_zip = inputs["TempFile"][0]
    input_zip_resource = input_zip["resource"]

    local_zip_path = pcs.storage.get_file(input_zip_resource)

    def extract_all_content(zip_file: str, dir: str) -> tuple:
        """
        Extract content from compressed file and return file names and output directory.
        """
        content_path = Path(pcs.storage.local_dir, dir)
        content_path.mkdir(exist_ok=True)

        with zipfile.ZipFile(zip_file) as zipObj:
            files = zipObj.namelist()
            zipObj.extractall(str(content_path))

        return files, content_path

    # extract `input_zip`
    imput_files, input_dir = extract_all_content(zip_file=local_zip_path, dir="input")
    pcs.debug([f"{len(imput_files)} input files @ {input_dir}"])

    # get files with extension `.xlsx` `.jpg` `.asd`
    files_excel_local = []
    files_jpg = []
    files_asd = []

    for (dirpath, dirnames, filenames) in os.walk(pcs.storage.local_dir):
        for f in filenames:
            if f.endswith(".xlsx") or f.endswith(".XLSX") or f.endswith(".xls") or f.endswith(".XLS"):
                files_excel_local.append(Path(dirpath, f))
            if f.endswith(".jpg") or f.endswith(".JPG"):
                files_jpg.append(pcs.storage.put_file(Path(dirpath, f)))
            if f.endswith(".asd") or f.endswith(".ASD"):
                files_asd.append(pcs.storage.put_file(Path(dirpath, f)))

    if len(files_excel_local) < 1:
        raise Exception(f"No found excel files")

    print(f"Found {len(files_excel_local)} files with extension `.xlsx`: {files_excel_local}")

    res_pd = pd.DataFrame()

    for excel_path in files_excel_local:
        column_photo_path = []
        column_spectre_path = []

        excel_data_fragment = pd.read_excel(excel_path, sheet_name="EJEMPLO")
        excel_data_fragment.rename(
            columns={
                "CÓDIGO": "Code",
                "DESCRIPCIÓN": "Description",
                "COORDENADAS X": "X-Coordinates",
                "COORDENADAS Y": "Y-Coordinates",
                "ALTITUD": "Altitude",
                "PENDIENTE": "Slope",
                "GRAVAS": "Gravels",
                "ARENAS MUY GRUESAS": "Very Coarse Sands",
                "ARENAS GRUESAS": "Coarse Sands",
                "ARENAS MEDIAS": "Medium Sands",
                "ARENAS FINAS": "Fine Sands",
                "ARENAS MUY FINAS": "Very Fine Sands",
                "ARENAS TOTALES": "Total Sands",
                "LIMOS GRUESOS": "Coarse Silts",
                "LIMOS FINOS": "Fine Silts",
                "LIMOS TOTALES": "Total Silts",
                "ARCILLAS": "Clays",
                "FACTOR K": "K Factor",
                "DENSIDAD APARENTE": "Apparent Density",
                "ESTABILIDAD DE AGREGADOS": "Aggregate Stability",
                "PERMEABILIDAD": "Permeability",
                "CAPACIDAD DE CAMPO": "Field Capacity",
                "PUNTO DE MARCHITEZ PERMANENTE": "Permanent Wilting Point",
                "HIDROFOBICIDAD": "Hydrofobicity",
                "CARBONO ORGÁNICO": "Organic Carbon",
                "FACTOR C": "C Factor",
                "CONDUCTIVIDAD ELÉCTRICA": "Electric Conductivity",
            },
            inplace=True,
        )

        column_photo = excel_data_fragment["FOTOGRAFÍAS"].values
        for row_photo in column_photo:
            strings_with_substring = [
                string for string in files_jpg if str(string).split("/")[-1].split(".")[0] in str(row_photo)
            ]
            if len(strings_with_substring) > 0:
                column_photo_path.append(strings_with_substring)
            else:
                column_photo_path.append(None)

        column_spectre = excel_data_fragment["RESPUESTA ESPECTRAL"].values
        for row_spectre in column_spectre:
            strings_with_substring = [
                string for string in files_asd if (str(string).split("/")[-1]).split(".")[0] in str(row_spectre)
            ]
            if len(strings_with_substring) > 0:
                column_spectre_path.append(strings_with_substring)
            else:
                column_spectre_path.append(None)

        excel_data_fragment["Pictures Path"] = column_photo_path
        excel_data_fragment["Spectral Response Path"] = column_spectre_path
        res_pd = pd.concat([res_pd, excel_data_fragment], ignore_index=True)
        res_pd.drop(columns=["FOTOGRAFÍAS", "RESPUESTA ESPECTRAL"], inplace=True)

        res_pd["Author"] = author
        res_pd["Group"] = group
        res_pd["Project"] = project

    # creates `out.json`
    out_path = Path(pcs.storage.local_dir, "out.json")
    with open(out_path, "w", encoding="utf-8") as file:
        res_pd.to_json(file, orient="records", indent=3, force_ascii=False)

    if not out_path.is_file():
        raise FileNotFoundError(f"`out.json` is missing")

    # send to remote storage
    dfs_dir = pcs.storage.put_file(out_path)
    pcs.info([f"Created file {out_path}"])

    # send to downstream
    output_json = TempFile(resource=dfs_dir)
    pcs.to_downstream(output_json)

    return TaskResult(files=[dfs_dir])
