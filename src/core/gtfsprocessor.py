import logging
import os
import csv
import zipfile
from activitysim.core import inject
from activitysim.core import pipeline
from activitysim.core import config

logger = logging.getLogger(__name__)

class GTFSProcessor:
    # class that interfaces between activitysim pipeline and gtfs files

    def __init__(self, filename):
        self.filename = filename
        self.contents = {} # dict of filename (no ext): list of columns

    def get_path(self):
        return config.data_file_path(self.filename)

    def tables(self):
        return self.contents.keys()

    def __capture_feed_tables(self):
        # records the filenames in the feed without extensions
        zipreader = zipfile.ZipFile(self.get_path(), 'r')
        filelist = zipreader.namelist()

        for filename in filelist:
            tablename = filename.split('.')[0]
            self.contents[tablename] = []
            # with zipreader.open(filename, 'r') as file:
            #     csvin = csv.reader(file)

    def __capture_feed_table_headers(self):
        # needs to be called after unzip
        for tablename in self.contents.keys():
            path = os.path.join(config.data_dir(), tablename + '.txt')
            with open(path, 'r', newline='') as file:
                csvin = csv.reader(file)
                self.contents[tablename] = next(csvin, [])

    def __validate_zipfile(self):
        filepath = self.get_path()

        assert os.path.exists(filepath), 'ERROR file not found at %s' % filepath
        assert zipfile.is_zipfile(filepath), 'ERROR %s not a valid zip file path' % filepath

    def __unzip(self):
        try:
            zip_reader = zipfile.ZipFile(self.get_path(), 'r')
            zip_reader.extractall(config.data_dir())
        except zipfile.BadZipFile:
            logger.error('Error: Bad zip file found at %s ' % filepath)
            return
        except FileNotFoundError:
            logger.error('Error: File not found at %s ' % filepath)
            return

    def __convert_to_csv(self):
        for tablename in self.contents:
            filename = tablename + '.txt'
            assert os.path.isfile(os.path.join(config.data_dir(), filename)), '%s not found in feed' % filename

            filename_no_extension = filename[:-4]
            csv_filename = filename_no_extension + '.csv'

            filepath = config.data_file_path(filename)
            dst_csv = os.path.join(config.data_dir(), csv_filename)

            os.rename(filepath, dst_csv)

    def cleanup_gtfs_files_in_data_dir(self):
        for tablename in self.contents:
            filename = tablename + '.csv'
            filepath = config.data_file_path(filename)
            os.remove(filepath)

    def write_tables_to_gtfs(self):
        # TODO
        return

    def initialize_gtfs_feed(self):
        self.__validate_zipfile()
        self.__capture_feed_tables()
        self.__unzip()
        self.__capture_feed_table_headers()
        self.__convert_to_csv()

