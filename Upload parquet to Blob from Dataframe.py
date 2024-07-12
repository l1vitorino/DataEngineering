import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime, timezone
import os
import logging

# Configuração do logger
logging.basicConfig(filename='upload_blob_cdc.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class UploadBlobCDC:
    """
    Args:
    - azure_storage_connection_string (str): String de conexão ao Azure Blob Storage. = "DefaultEndpointsProtocol=https;AccountName=ststudie..."
    - container_name (str): Nome do container no Azure Blob Storage. = "Brazil"
    - blob_name (str): Nome base do blob. = "/Man/TabelaA/CDC/"
    - file_name (str): Nome do arquivo. = "TblVendas"
    - CDC_date (datetime): Data e hora da extração do arquivo. = "datetime.now()"
    - df (pandas.DataFrame): DataFrame a ser convertido em Parquet e enviado ao Blob Storage.
    """
    def __init__(self,azure_storage_connection_string, container_name, blob_name, file_name, CDC_date, df):
        self.azure_storage_connection_string = azure_storage_connection_string
        self.container_name = container_name
        self.blob_name = blob_name
        self.file_name = file_name
        self.df = df
        self.parquet_file =  f'{self.blob_name}{CDC_date.strftime("%y-%m-%d")}/{self.file_name}{CDC_date.strftime("_%H:%M:%S.%f")}.parquet'
        self.Temp_parquet_file =  f'Temp{self.file_name}.parquet'


    def dftoparquet(self) -> None:
        try:
            # Convertendo o DataFrame para Parquet
            table = pa.Table.from_pandas(self.df)
            pq.write_table(table,  self.Temp_parquet_file)
        except Exception as e:
            logging.error(f'Ocorreu um erro: {str(e)}')

    def ParquettoBlob(self) -> None:
        # Conectando ao serviço Blob Storage
        try:
            blob_client = (BlobServiceClient.
                                    from_connection_string(self.azure_storage_connection_string)
                                    .get_container_client(self.container_name)
                                    .get_blob_client(self.parquet_file)
            )

            # Enviando o arquivo Parquet para o Blob Storage
            with open(self.Temp_parquet_file, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
        except Exception as e:
            logging.error(f'Ocorreu um erro: {str(e)}')
    
    def removetempfile(self) -> None:
        try:
        # Removendo o arquivo temporário local
            os.remove(self.Temp_parquet_file)
        except Exception as e:
            logging.error(f'Ocorreu um erro: {str(e)}')

    def execute(self) -> None:
        self.dftoparquet()
        self.ParquettoBlob()
        self.removetempfile()

        
        
