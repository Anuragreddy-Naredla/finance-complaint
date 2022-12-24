from finance_complaint.exception import FinanceException
import os, sys

from finance_complaint.utils import read_yaml_file, write_yaml_file
from collections import namedtuple
from finance_complaint.logger import logger
from finance_complaint.cloud_storage import SimpleStorageService
DataIngestionMetadataInfo = namedtuple("DataIngestionMetadataInfo", ["from_date", "to_date", "data_file_path"])


class DataIngestionMetadata:

    def __init__(self, metadata_file_path,):
        self.metadata_file_path = metadata_file_path



    @property
    def is_metadata_file_present(self):
        """
        This method is used to check the meta_info.yaml file is present or not

        Returns:
            Bool: True or False
        """
        return os.path.exists(self.metadata_file_path)

    def write_metadata_info(self, from_date: str, to_date: str, data_file_path: str):
        try:
            metadata_info = DataIngestionMetadataInfo(
                from_date=from_date,
                to_date=to_date,
                data_file_path=data_file_path
            )
            write_yaml_file(file_path=self.metadata_file_path, data=metadata_info._asdict())

        except Exception as e:
            raise FinanceException(e, sys)

    def get_metadata_info(self) -> DataIngestionMetadataInfo:
        """
        This method is used to get the metadata information

        Raises:
            Exception: _description_
            FinanceException: _description_

        Returns:
            DataIngestionMetadataInfo: _description_
        """
        try:
            # If file is not present raising an error if file is there reading the yaml file.
            if not self.is_metadata_file_present:
                raise Exception("No metadata file available")
            metadata = read_yaml_file(self.metadata_file_path)
            metadata_info = DataIngestionMetadataInfo(**(metadata))
            logger.info(metadata)
            return metadata_info
        except Exception as e:
            raise FinanceException(e, sys)
