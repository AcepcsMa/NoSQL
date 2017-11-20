__author__ = 'Ma Haoxiang'

# import
import time
import threading
from Response import responseCode


class timer(threading.Thread):

    def __init__(self, database, saveInterval=60):
        super(timer, self).__init__()
        self.saveInterval = saveInterval
        self.database = database[0]

    # make the response message
    def makeMessage(self, msg, typeCode, data):
        message = {
            "msg": msg,
            "typeCode": typeCode,
            "data": data
        }
        return message

    def setInterval(self, interval):
        self.saveInterval = interval
        self.database.saveDb()
        msg = self.makeMessage(responseCode.detail[responseCode.SAVE_INTERVAL_CHANGE_SUCCESS],
                               responseCode.SAVE_INTERVAL_CHANGE_SUCCESS,
                               interval)
        return msg

    def run(self):
        while True:
            self.database.saveDb()
            time.sleep(self.saveInterval)
