import io
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import os 

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

api_url_list= [
'https://www.mibici.net/site/assets/files/1230/datos_abiertos_2020_01.csv',
'https://www.mibici.net/site/assets/files/1231/datos_abiertos_2020_02.csv',
'https://www.mibici.net/site/assets/files/1232/datos_abiertos_2020_03.csv',
'https://www.mibici.net/site/assets/files/1235/datos_abiertos_2020_04.csv',
'https://www.mibici.net/site/assets/files/1236/datos_abiertos_2020_05.csv',
'https://www.mibici.net/site/assets/files/1237/datos_abiertos_2020_06.csv',
'https://www.mibici.net/site/assets/files/1238/datos_abiertos_2020_07.csv',
'https://www.mibici.net/site/assets/files/1239/datos_abiertos_2020_08.csv',
'https://www.mibici.net/site/assets/files/1240/datos_abiertos_2020_09.csv',
'https://www.mibici.net/site/assets/files/1241/datos_abiertos_2020_10.csv',
'https://www.mibici.net/site/assets/files/1317/datos_abiertos_2020_11.csv',
'https://www.mibici.net/site/assets/files/1318/datos_abiertos_2020_12.csv',
'https://www.mibici.net/site/assets/files/1320/datos_abiertos_2021_01.csv',
'https://www.mibici.net/site/assets/files/1323/datos_abiertos_2021_02.csv',
'https://www.mibici.net/site/assets/files/1322/datos_abiertos_2021_03.csv',
'https://www.mibici.net/site/assets/files/2129/datos_abiertos_2021_04.csv',
'https://www.mibici.net/site/assets/files/3292/datos_abiertos_2021_05.csv',
'https://www.mibici.net/site/assets/files/4572/datos_abiertos_2021_06.csv',
'https://www.mibici.net/site/assets/files/5728/datos_abiertos_2021_07.csv',
'https://www.mibici.net/site/assets/files/7073/datos_abiertos_2021_08.csv',
'https://www.mibici.net/site/assets/files/8461/datos_abiertos_2021_09.csv',
'https://www.mibici.net/site/assets/files/10088/datos_abiertos_2021_10.csv',
'https://www.mibici.net/site/assets/files/11538/datos_abiertos_2021_11.csv',
'https://www.mibici.net/site/assets/files/12780/datos_abiertos_2021_12.csv',
'https://www.mibici.net/site/assets/files/14797/datos_abiertos_2022_01.csv',
'https://www.mibici.net/site/assets/files/16831/datos_abiertos_2022_02.csv',
'https://www.mibici.net/site/assets/files/19034/datos_abiertos_2022_03.csv',
'https://www.mibici.net/site/assets/files/20338/datos_abiertos_2022_04.csv',
'https://www.mibici.net/site/assets/files/21842/datos_abiertos_2022_05.csv',
'https://www.mibici.net/site/assets/files/23473/datos_abiertos_2022_06.csv',
'https://www.mibici.net/site/assets/files/25235/datos_abiertos_2022_07.csv',
'https://www.mibici.net/site/assets/files/27484/datos_abiertos_2022_08.csv',
'https://www.mibici.net/site/assets/files/29663/datos_abiertos_2022_09.csv',
'https://www.mibici.net/site/assets/files/31507/datos_abiertos_2022_10.csv',
'https://www.mibici.net/site/assets/files/33115/datos_abiertos_2022_11.csv',
'https://www.mibici.net/site/assets/files/34432/datos_abiertos_2022_12.csv',
'https://www.mibici.net/site/assets/files/36762/datos_abiertos_2023_01.csv',
'https://www.mibici.net/site/assets/files/40197/datos_abiertos_2023_02.csv',
'https://www.mibici.net/site/assets/files/43035/datos_abiertos_2023_03.csv',
'https://www.mibici.net/site/assets/files/46758/datos_abiertos_2023_04.csv',
'https://www.mibici.net/site/assets/files/46759/datos_abiertos_2023_05.csv',
'https://www.mibici.net/site/assets/files/48116/datos_abiertos_2023_06.csv',
'https://www.mibici.net/site/assets/files/49448/datos_abiertos_2023_07.csv',
'https://www.mibici.net/site/assets/files/51295/datos_abiertos_2023_08.csv',
'https://www.mibici.net/site/assets/files/53344/datos_abiertos_2023_09.csv',
'https://www.mibici.net/site/assets/files/54935/datos_abiertos_2023_10.csv',
'https://www.mibici.net/site/assets/files/57344/datos_abiertos_2023_11.csv',
'https://www.mibici.net/site/assets/files/58715/datos_abiertos_2023_12.csv',
'https://www.mibici.net/site/assets/files/61450/datos_abiertos_2024_01.csv',
'https://www.mibici.net/site/assets/files/77251/datos_abiertos_2024_02.csv',
'https://www.mibici.net/site/assets/files/77252/datos_abiertos_2024_03.csv'
]

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/my-creds.json'

# function to yield one file at a time to download
def file_generator(api_url_list):
    for file_url in api_url_list:
        filename = file_url.split('/')[-1]
        print(f'Processing files: {filename}') 
        os.system(f"wget -q {file_url}")
        bike_dtypes = {
        'Viaje_Id': pd.Int64Dtype(),
        'Usuario_Id': pd.Int64Dtype(), 
        'Genero':str,
        'AÃ±o_de_nacimiento': pd.Int64Dtype(), 
        'Origen_Id':pd.Int64Dtype(),
        'Destino_Id':pd.Int64Dtype() 
        }
        parse_dates = ['Inicio_del_viaje', 'Fin_del_viaje']
        pdf = pd.read_csv(filename,dtype=bike_dtypes,parse_dates=parse_dates,encoding='latin-1')
        yield pdf        
        os.remove(filename)


@data_loader
def load_data_from_api(*args, **kwargs):
    bucket_name = kwargs['context']['bucket_name']
    project_id = kwargs['context']['project_id']
    table_name= 'bike_dataset'
    root_path=f'{bucket_name}/{table_name}/raw'
# Iterate over files using the generator
    for pdf in file_generator(api_url_list):
        # print("Processing files:")  
        translations = {
            'Viaje_Id':'Trip_Id',
            'Usuario_Id':'User_Id',
            'Genero' : 'Gender',
            'AÃ±o_de_nacimiento' : 'Year_of_Birth',
            'Inicio_del_viaje' : 'Trip_starttime',
            'Fin_del_viaje' : 'Trip_endtime',
            'Origen_Id' : 'Origin_Id',
            'Destino_Id' : 'Destination_Id',
        } 
        pdf.rename(columns=translations, inplace=True) 
        pdf['year'] = pdf['Trip_starttime'].dt.year
        pdf['month'] = pdf['Trip_starttime'].dt.month

        table = pa.Table.from_pandas(pdf)
        gcs = pa.fs.GcsFileSystem()

        pq.write_to_dataset(
            table,
            root_path=f'{root_path}/rides',
            partition_cols=['year','month'],
            filesystem=gcs,
            coerce_timestamps='ms',
            allow_truncated_timestamps=True,
            existing_data_behavior='delete_matching'
        ) 

    # ingest nomcenclature dataset 
    stations_url = 'https://www.mibici.net/site/assets/files/1118/nomenclatura_2024_03.csv'
    os.system(f"wget -q {stations_url}")
    stations_dtypes = {
        'name': 'string',
        'obcn': 'string',
        'location': 'string',
        'latitude': 'float64',
        'longitude': 'float64',
        'status': 'string',
        'id': 'int64'
    }
    filename = stations_url.split('/')[-1]
    cwd = os.getcwd()

    # Print the current working directory
    print("Current working directory:", cwd)
    pdf = pd.read_csv(filename,dtype=stations_dtypes,encoding='latin-1')

    stations_Table = pa.Table.from_pandas(pdf)
    # gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table=stations_Table,
        root_path=f'{root_path}/nomenclature',
        filesystem=gcs, 
        coerce_timestamps='ms',
        allow_truncated_timestamps=True,
        existing_data_behavior='delete_matching'
    ) 
    os.remove(filename)
    return {}
# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'