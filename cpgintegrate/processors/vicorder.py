import pandas
from pywinauto.application import Application, ProcessNotFoundError
from pywinauto.findwindows import  ElementNotFoundError
from pywinauto import Desktop
import os
import tempfile
import patoolib
import re
import glob
from pywinauto import timings


def to_frame(zip_file, exe_path='C:/Program Files (x86)/Skidmore Medical/Vicorder/Vicorder.exe'):
    """
    Turns Vicorder exports into dataframes using to_frame as long as Skidmore Medical Vicorder software is installed

    Works with version 8.0.6353, all others at your own risk.

    :param zip_file: A file-like of an archive that contains a vicorder import
    :param exe_path: Path to Vicorder.exe
    :return:
    """

    try:
        app = Application().connect(path=exe_path)
    except ProcessNotFoundError:
        app = Application().start(exe_path)

    app.top_window()

    # Persist because Vicorder software wigs out if database location doesn't exist
    temp_data_dir = tempfile.mkdtemp()

    # Switch to a temporary database because deleting patients
    #  leads to their exams showing up in subsequent imports

    main_window = app['Reader Station - [Administration]']

    try:
        main_window.wait('active', 10)
    except timings.TimeoutError:
        app.kill()
        app = Application().start(exe_path)
        main_window = app['Reader Station - [Administration]']
        main_window.wait('active', 10)

    main_window['Database Utilities'].click_input()

    util = Desktop()['Database Utilities']
    util['Change Database Location'].click_input()
    Desktop()['Save As'].wait('exists')
    Desktop()['Save As'].Edit.set_text(os.path.normcase(temp_data_dir + '/DopData.db'))
    Desktop()['Save As'].Save.click_input()

    for func in ['No', 'Yes', 'OK']:
                Desktop().Vicorder.wait('exists', 20)
                Desktop().Vicorder[func].click_input()

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

        main_window = app['Reader Station - [Administration]']
        main_window.wait('exists')
        main_window.Import.click_input()
        app.Open.wait('exists')
        app.Open.Edit.set_text(
            next(f for f in glob.iglob(temp_dir+'/**/*.xml*', recursive=True) if os.path.isfile(f)))
        app.Open.Open.click_input()
        try:
            app.Vicorder.wait('exists')
        except timings.TimeoutError:
            app.Error.OK.click_input()
        app.Vicorder.OK.click_input()

        main_window.ListView2.Select(1).click_input()

        csv_temp_file = os.path.join(temp_dir, 'vicorder.csv')

        exams_list = main_window.ListView
        first_exam = True
        for i in range(exams_list.ItemCount()):
            main_window.wait('exists')
            exams_list.Select(i).click_input(button='left', double=True)
            try:
                main_window.wait_not('exists')
                app.window(title_re='Reader Station.*').Save.click_input()
            except ElementNotFoundError:
                app.Vicorder.OK.click_input()
                app.window(title_re='Reader Station.*').Save.click_input()
            except timings.TimeoutError:
                app.Vicorder.OK.click_input()
                continue
            if first_exam:
                save_dialog = app.Dialog
                save_dialog.Edit.set_text(csv_temp_file)
                save_dialog.Save.click_input()
                first_exam = False
            app.window(title_re='Reader Station.*').wait('active')
            app.window(title_re='Reader Station.*')['Close'].click_input()

        df = pandas.read_csv(csv_temp_file)

        return df
