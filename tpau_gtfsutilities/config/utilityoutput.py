import os
import zipfile
from datetime import datetime

class _UtilityOutput:
    utility = None
    dir_index = 0
    parent_output_dir = None
    _feedname = None

    def initialize_utility(self, utility):
        # utiltiy is one of:
        #   average_headways
        #   one_day
        #   etc.

        self.utility = utility
        self.create_utilty_output_dir()

    def set_parent_output_dir(self, dir):
        self.parent_output_dir = dir

    def get_parent_output_dir(self):
        return self.parent_output_dir if self.parent_output_dir else 'output'

    def set_feedname(self, feedname):
        self._feedname = feedname

    def create_utilty_output_dir(self):
        while (os.path.exists(self.get_utility_output_dir())):
            self.dir_index += 1
        os.mkdir(self.get_utility_output_dir())

    def get_utility_output_dir(self):
        dirname = self.utility + '_' + str(self.dir_index) if self.dir_index > 0 else self.utility
        return os.path.join(self.get_parent_output_dir(), dirname)
    
    def get_output_dir(self):
        if self._feedname:
            out_dir = os.path.join(self.get_utility_output_dir(), self._feedname)
        else:
            out_dir = self.get_utility_output_dir()
        if not (os.path.exists(out_dir)):
            os.mkdir(out_dir)
        return out_dir

    def write_metadata(self, settings):
        filename = 'metadata.txt'
        f = open(os.path.join(self.get_utility_output_dir(), filename), 'a')
        timestamp = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        f.write('Created at ' + timestamp + '\n')
        f.write('Utility: ' + self.utility + '\n')
        for setting in settings:
            value = str(settings[setting])
            text = setting + ': ' + value + '\n'
            f.write(text)

    def write_or_append_to_output_csv(self, df, filename, index=False):
        csvfile = os.path.join(self.get_output_dir(), filename)
        if os.path.exists(csvfile):
            df.to_csv(csvfile, mode='a', header=False, index=index)
        else:
            df.to_csv(csvfile, index=index)
        
    def write_to_zip(self, df_dict, feedname):
        # df_dict: dict of dataframess by tablename

        feed_folder = os.path.join(self.get_output_dir(), feedname)

        def write_txt_to_folder(tablename, df):
            filename = tablename + '.txt'
            df.to_csv(os.path.join(feed_folder, filename), index=False)

        def write_feed_txts():
            os.mkdir(feed_folder)
            for table in df_dict:
                write_txt_to_folder(table, df_dict[table])

        def zip_feed():
            zip_path = os.path.join(self.get_output_dir(), feedname + '.zip')
            zip_writer = zipfile.ZipFile(zip_path, 'w')

            for root, dirs, files in os.walk(feed_folder):
                for file in files:
                    zip_writer.write(os.path.join(root, file),os.path.basename(file), compress_type = zipfile.ZIP_DEFLATED)

            zip_writer.close()

        def cleanup():
            for table in df_dict:
                txt_file = os.path.join(feed_folder, table + '.txt')
                if os.path.exists(txt_file):
                    os.remove(txt_file)
            try:
                os.rmdir(feed_folder)
            except (FileNotFoundError, OSError) as e:
                print ("Error: could not clean up directory %s - %s." % (e.filename, e.strerror))

        write_feed_txts()
        zip_feed()
        cleanup()

utilityoutput = _UtilityOutput()