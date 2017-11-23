__author__ = 'Ma Haoxiang'

# import
import time
import threading
from Response import responseCode
from Utils import Utils

class Timer(threading.Thread):

    def __init__(self, database, saveInterval=60):
        super(Timer, self).__init__()
        self.saveInterval = saveInterval
        self.database = database[0]

    def setInterval(self, interval):
        self.saveInterval = interval
        self.database.saveDb()
        msg = Utils.makeMessage(responseCode.detail[responseCode.SAVE_INTERVAL_CHANGE_SUCCESS],
                               responseCode.SAVE_INTERVAL_CHANGE_SUCCESS,
                               interval)
        return msg

    def run(self):
        while True:
            self.database.saveDb()
            time.sleep(self.saveInterval)
