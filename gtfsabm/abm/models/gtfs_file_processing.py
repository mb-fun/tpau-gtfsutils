import logging
import os
import zipfile
from activitysim.core import inject
from activitysim.core import pipeline
from activitysim.core import config

logger = logging.getLogger(__name__)

def unzip(file):
    assert os.path.exists(file), 'file not found: %s' % file
    assert file_extension_matches(file, '.zip'), 'file not a zip file: %s' % file

    if not zipfile.is_zipfile(file):
        logger.error('Error: %s not a valid zip file path' % file)
        return
    try:
        zip_reader = zipfile.ZipFile(file, 'r')
        zip_reader.extractall(config.data_dir())
    except zipfile.BadZipFile:
        logger.error('Error: Bad zip file found at %s ' % file)
        return
    except FileNotFoundError:
        logger.error('Error: File not found at %s ' % file)
        return

def file_in_data_dir(filename):
    return os.path.exists(config.data_file_path(filename))

def convert_txt_file_to_csv(filename):
    assert file_extension_matches(filename, '.txt'), '%s is not a txt file' % filename
    assert file_in_data_dir(filename), 'txt file %s not found in data directory'

    filename_no_extension = filename[:-4]
    new_filename = filename_no_extension + '.csv'

    source_txt = config.data_file_path(filename)
    dst_csv = os.path.join(config.data_dir(), new_filename)

    os.rename(source_txt, dst_csv)

def file_extension_matches(filepath, extension):
    filename, file_extension = os.path.splitext(filepath)
    return file_extension == extension

def convert_all_input_txts_to_csv():
    all_files_in_data = os.listdir(config.data_dir())
    for file in all_files_in_data:
        if file_extension_matches(file, '.txt'):
            convert_txt_file_to_csv(file)

def prepare_csv_gtfs_data():
    gtfs_filename = config.setting('feed_name')
    logger.info('gtfs_filename: %s ' % gtfs_filename)
    gtfs_filepath = config.data_file_path(gtfs_filename)

    unzip(gtfs_filepath)
    convert_all_input_txts_to_csv()

@inject.step()
def initialize_gtfs_tables():
    prepare_csv_gtfs_data()
