import pandas


class ExtensionCheck:

    def __init__(self, extension):
        """
        Throws a NameError if file name doesn't match extension(s) given

        :param extension: str or iterable of str
        """
        self.extension_list = [l.lower() for l in ([extension] if type(extension) == str else extension)]

    def to_frame(self, file):
        for ext in self.extension_list:
            if file.name.lower().endswith(ext):
                return pandas.DataFrame(index=[0])
        raise NameError("Extension not in list")
