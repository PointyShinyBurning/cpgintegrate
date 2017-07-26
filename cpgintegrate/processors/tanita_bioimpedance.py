import pandas


def to_frame(file):
    """Turns CSV export from Tanita BODY COMPOSITION ANALYZER BC-418 into a DataFrame

    :param file: A file-like object that has a .name
    :return: A DataFrame containing its variables arranged as "LINEHEADING_VARNAME" with the subject ID as the index
    """
    sheet = pandas.DataFrame()

    file = file.read().decode('utf-16').splitlines()
    line_num = 0
    while line_num < len(file):
        if file[line_num].strip() != "":
            head = file[line_num].strip().split("\t")
            # Find last repeat of measure
            try:
                while file[line_num + 1].strip() != "":
                    line_num += 1
            except IndexError:
                # Run off end of file
                pass
            dat = file[line_num].strip().split("\t")

            data = pandas.DataFrame({h: d for h, d in zip(head, dat)}, columns=head, index=[0])

            ren = [data.columns[0] + "_" + col for col in data.columns[1:]]
            data.columns = [data.columns[0]] + ren

            sheet = pandas.concat([sheet, data], axis=1)
        line_num += 1

    assert sheet['BMI_WEIGHT'][0] == sheet['BMI_WEIGHT'][0]

    return sheet.set_index("CUSTOMER_CUSTOMERID", drop=False)
