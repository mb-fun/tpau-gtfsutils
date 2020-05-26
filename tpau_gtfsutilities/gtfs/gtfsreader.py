import logging
import os
import csv
import zipfile
from tpau_gtfsutilities.config.utilityconfig import utilityconfig

logger = logging.getLogger(__name__)

class GTFSReader:
    def __init__(self, filename):
        self.filename = filename
        self.contents = {} # dict of filename (no ext): list of columns

    def get_path(self):
        return os.path.join(utilityconfig.input_dir(), self.filename)

    def tables(self):
        return self.contents.keys()

    def update_table_columns(self, tablename, columns):
        self.contents[tablename] = columns

    def __capture_feed_tables(self):
        # records the filenames in the feed without extensions
        zipreader = zipfile.ZipFile(self.get_path(), 'r')
        filelist = zipreader.namelist()

        for filename in filelist:
            tablename = filename.split('.')[0]
            self.contents[tablename] = []

    def __capture_feed_table_headers(self):
        # needs to be called after unzip
        for tablename in self.contents.keys():
            path = os.path.join(utilityconfig.input_dir(), tablename + '.txt')
            with open(path, 'r', newline='') as file:
                csvin = csv.reader(file)
                headers = next(csvin, [])
                self.contents[tablename] = headers

    def __validate_zipfile(self):
        filepath = self.get_path()

        assert os.path.exists(filepath), 'ERROR file not found at %s' % filepath
        assert zipfile.is_zipfile(filepath), 'ERROR %s not a valid zip file path' % filepath

    def __unzip(self):
        try:
            zip_reader = zipfile.ZipFile(self.get_path(), 'r')
            zip_reader.extractall(utilityconfig.input_dir())
        except zipfile.BadZipFile:
            logger.error('Error: Bad zip file found at %s ' % filepath)
            return
        except FileNotFoundError:
            logger.error('Error: File not found at %s ' % filepath)
            return

    def __convert_to_csv(self):
        for tablename in self.contents:
            filename = tablename + '.txt'
            assert os.path.isfile(os.path.join(utilityconfig.input_dir(), filename)), '%s not found in feed' % filename

            filename_no_extension = filename[:-4]
            csv_filename = filename_no_extension + '.csv'

            filepath = os.path.join(utilityconfig.input_dir(), filename)
            dst_csv = os.path.join(utilityconfig.input_dir(), csv_filename)

            os.rename(filepath, dst_csv)

    def cleanup_gtfs_files_in_data_dir(self):
        for tablename in self.contents:
            filename = tablename + '.csv'
            filepath = os.path.join(utilityconfig.input_dir(), filename)
            os.remove(filepath)


    def unpack_csvs(self):
        self.__validate_zipfile()
        self.__capture_feed_tables()
        self.__unzip()
        self.__capture_feed_table_headers()
        self.__convert_to_csv()

