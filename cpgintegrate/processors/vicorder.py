import pandas
from pywinauto.application import Application
from pywinauto import Desktop
import os
import tempfile
import patoolib
import re
import glob
from pywinauto import timings


class Vicorder:

    def __init__(self, exe_path='C:/Program Files (x86)/Skidmore Medical/Vicorder/Vicorder.exe'):
        """
        Turns Vicorder exports into dataframes using to_frame as long as Skidmore Medical Vicorder software is installed

        Works with version 8.0.6353, all others at your own risk.

        :param exe_path: Path to Vicorder.exe
        """
        self.app = Application().start(exe_path)
        self.app.top_window()
        timings.Timings.Slow()

    def to_frame(self, zip_file):
        """

        :param zip_file: A file-like of an archive that contains a vicorder import
        :return:
        """

        # Persist because Vicorder software wigs out if database location doesn't exist
        temp_data_dir = tempfile.mkdtemp()

        # Switch to a temporary database because deleting patients
        #  leads to their exams showing up in subsequent imports
        main_window = self.app['Reader Station - [Administration]']
        main_window['Database Utilities'].click_input()

        util = Desktop()['Database Utilities']
        util['Change Database Location'].click_input()
        Desktop()['Save As'].wait('exists')
        Desktop()['Save As'].Edit.set_text(os.path.normcase(temp_data_dir + '/DopData.db'))
        Desktop()['Save As'].Save.click_input()

        Desktop().Vicorder.No.click_input()
        Desktop().Vicorder.Yes.click_input()
        Desktop().Vicorder.OK.click_input()

        Desktop()['Save As'].wait('exists')
        os.makedirs(temp_data_dir + '/Backup')
        Desktop()['Save As'].Edit.set_text(os.path.normcase(temp_data_dir + '/Backup/DopData.bk'))
        Desktop()['Save As'].Save.click_input()

        util.exit.click_input()

        with tempfile.TemporaryDirectory() as temp_dir:
            if hasattr(zip_file, 'name'):
                zip_file_name = re.split('[/\\\\]', zip_file.name)[-1]
            else:
                zip_file_name = 'vicorder.zip'

            zip_file_path = os.path.join(temp_dir, zip_file_name)

            with open(zip_file_path, 'wb') as temp_zip_file:
                temp_zip_file.write(zip_file.read())

            patoolib.extract_archive(zip_file_path, outdir=temp_dir)
            os.remove(zip_file_path)

            main_window = self.app['Reader Station - [Administration]']
            main_window.wait('exists')
            main_window.Import.click_input()
            self.app.Open.wait('exists')
            self.app.Open.Edit.set_text(next(glob.iglob(temp_dir+'/**/*.xml', recursive=True)))
            self.app.Open.Open.click_input()
            self.app.Vicorder.wait('exists')
            self.app.Vicorder.OK.click_input()

            main_window.ListView2.Select(1).click_input()

            csv_temp_file = os.path.join(temp_data_dir, 'vicorder.csv')

            exams_list = main_window.ListView
            first_exam = True
            for i in range(exams_list.ItemCount()):
                main_window.wait('exists')
                exams_list.Select(i).click_input(button='left', double=True)
                main_window.wait_not('exists')
                self.app.window(title_re='Reader Station.*').Save.click_input()
                if first_exam:
                    save_dialog = self.app.Dialog
                    save_dialog.Edit.set_text(csv_temp_file)
                    save_dialog.Save.click_input()
                    first_exam = False
                self.app.window(title_re='Reader Station.*').wait('active')
                self.app.window(title_re='Reader Station.*')['Close'].click_input()

            df = pandas.read_csv(csv_temp_file)

            os.remove(csv_temp_file)

            return df
