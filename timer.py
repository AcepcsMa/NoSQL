__author__ = 'Ma Haoxiang'

# import
import time
import threading


class timer(threading.Thread):

    def __init__(self, database, saveInterval=60):
        super(timer,self).__init__()
        self.saveInterval = saveInterval
        self.database = database[0]

    def setInterval(self, interval):
        self.saveInterval = interval

    def run(self):
        while(True):
            self.database.saveDb()
            time.sleep(self.saveInterval)
