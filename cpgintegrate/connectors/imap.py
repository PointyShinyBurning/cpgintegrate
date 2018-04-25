import typing
import pandas
from .connector import FileDownloadingConnector
import imaplib
import email
import io


class IMAP(FileDownloadingConnector):

    def __init__(self, host, auth: (str, str), **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.auth = auth
        self.mail = None

    def get_dataset(self, mailbox, folder):
        self.mail = imaplib.IMAP4_SSL(self.host)
        self.mail.login(self.auth[0] + '\\' + mailbox, self.auth[1])
        self.mail.select(folder, readonly=True)
        result, data = self.mail.uid('search', None, 'ALL')
        return pandas.DataFrame({'uid': data[0].split()})

    def iter_files(self, mailbox, folder) -> typing.Iterator[typing.IO]:
        """
        Returns file-like of the first attachment from each message
        :param mailbox:
        :param folder:
        :return:
        """
        msgs = self.get_dataset(mailbox, folder)
        uidvalidity = self.mail.status(folder, '(UIDVALIDITY)')[1][0].decode('UTF-8')
        for uid in msgs.uid:
            result, data = self.mail.uid('fetch', uid, '(RFC822)')
            msg = email.message_from_bytes(data[0][1])
            attch = io.BytesIO(msg.get_payload()[1].get_payload(decode=True))
            attch.name = 'imap://{0}:{1}/{2}[{3}]{4}[1]'.format(mailbox, self.host, folder, uidvalidity,
                                                                uid.decode('ASCII'))
            yield attch
