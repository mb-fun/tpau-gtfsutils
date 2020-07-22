import os
import zipfile

class _UtilityOutput:
    utility = None
    dir_index = 0

    def initialize_utility(self, utility):
        # utiltiy is one of:
        #   average_headways
        #   one_day

        self.utility = utility
        self.create_output_dir()

    def create_output_dir(self):
        while (os.path.exists(self.get_output_dir())):
            self.dir_index += 1
        os.mkdir(self.get_output_dir())

    def get_output_dir(self):
        parentdir = 'output'
        dirname = self.utility + '_' + str(self.dir_index) if self.dir_index > 0 else self.utility
        return os.path.join(parentdir, dirname)

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