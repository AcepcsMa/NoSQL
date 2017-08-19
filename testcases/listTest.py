__author__ = 'Ma Haoxiang'

import requests
import json
import time

class listTest:
    def __init__(self):
        self.host, self.port = self.readTestConfig()

    # read test configuration from test.conf
    def readTestConfig(self):
        with open("test.conf", "r") as configFile:
            config = json.loads(configFile.read())
            try:
                host = config["HOST"]
                port = config["PORT"]
                return host, port
            except:
                return None, None

    # write test log
    def writeLog(self, url, params, response):
        with open("testLog.log", "a") as testLogFile:
            testLogFile.write("DATE: " + time.strftime("%Y/%m/%d %H:%M:%S",time.localtime(time.time())))
            testLogFile.write("\n")
            testLogFile.write("URL: " + url)
            testLogFile.write("\n")
            testLogFile.write("PARAMS: " + params)
            testLogFile.write("\n")
            testLogFile.write("RESPONSE: " + response)
            testLogFile.write("\n")

    # test create list function
    def createListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"

        # case1 create a list
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case2 create a list repeatedly
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db100","list1"))
        self.writeLog(url.format("db100","list1"),"",response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/makelist/{0}/{1}"
        response = requests.get(errorUrl.format("db0","list2"))
        self.writeLog(errorUrl.format("db0","list2"),"",response.content.decode())

    def getListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getList/{0}/{1}"

        # case1 create a list and get its value
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        response = requests.get(createUrl.format("db0","list1"))
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db999","list1"))
        self.writeLog(url.format("db999","list1"),"",response.content.decode())

        # case3 unknown list name
        response = requests.get(url.format("db0","list999"))
        self.writeLog(url.format("db0","list999"),"",response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getlist/{0}/{1}"
        response = requests.get(errorUrl.format("db0","list1"))
        self.writeLog(errorUrl.format("db0","list1"),"",response.content.decode())

    def insertListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/insertList"

        # case1 create a list and then insert
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        response = requests.get(createUrl.format("db0", "list1"))
        params = {
            "dbName":"db0",
            "listName":"list1",
            "listValue":123
        }
        response = requests.post(url,json=params)
        self.writeLog(url,json.dumps(params),response.content.decode())

        # case2 insert repeatedly
        response = requests.post(url,json=params)
        self.writeLog(url,json.dumps(params),response.content.decode())

        # case3 unknown database name
        errorParams = {
            "dbName": "db100",
            "listName": "list1",
            "listValue": 123
        }
        response = requests.post(url,json=errorParams)
        self.writeLog(url,json.dumps(errorParams),response.content.decode())

        # case4 unknown list name
        errorParams = {
            "dbName": "db0",
            "listName": "list999",
            "listValue": 123
        }
        response = requests.post(url,json=errorParams)
        self.writeLog(url,json.dumps(errorParams),response.content.decode())

        # case5 different data type
        params = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": [1,2,"hello"]
        }
        response = requests.post(url,json=params)
        self.writeLog(url,json.dumps(params),response.content.decode())

        # case6 error database type
        errorParams = {
            "dbName": [1,2,3],
            "listName": "list999",
            "listValue": 123
        }
        response = requests.post(url,json=errorParams)
        self.writeLog(url,json.dumps(errorParams),response.content.decode())

        # case7 error list type
        errorParams = {
            "dbName": "db0",
            "listName": [4,5,6],
            "listValue": 123
        }
        response = requests.post(url,json=errorParams)
        self.writeLog(url,json.dumps(errorParams),response.content.decode())

        # case8 error url
        errorParams = {
            "dbName": "db0",
            "listName": "list999",
            "listValue": 123
        }
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/insertlist"
        response = requests.post(errorUrl,json=errorParams)
        self.writeLog(errorUrl,json.dumps(errorParams),response.content.decode())

    def deleteListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/deleteList/{0}/{1}"

        # case1 create a list and then delete
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        response = requests.get(createUrl.format("db0", "list1"))
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case2 delete repeatedly
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db100","list1"))
        self.writeLog(url.format("db100","list1"),"",response.content.decode())

        # case4 unknown list name
        response = requests.get(url.format("db0","list111"))
        self.writeLog(url.format("db0","list111"),"",response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/deletelist/{0}/{1}"
        response = requests.get(errorUrl.format("db0","list1"))
        self.writeLog(errorUrl.format("db0","list1"),"",response.content.decode())

    def rmListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rmFromList"

        # case1 create a list, insert some values and then remove
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertList"

        response = requests.get(createUrl.format("db0", "list1"))
        params = {
            "dbName":"db0",
            "listName":"list1",
            "listValue":"hello"
        }
        response = requests.post(insertUrl,json=params)
        removeParams = params
        response = requests.post(url,json=removeParams)
        self.writeLog(url,json.dumps(removeParams),response.content.decode())

        # case2 remove repeatedly
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case3 unknown database name
        removeParams = {
            "dbName":"db100",
            "listName":"list1",
            "listValue":"hello"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case4 unknown list name
        removeParams = {
            "dbName": "db0",
            "listName": "list111",
            "listValue": "hello"
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case5 non-existed value
        removeParams = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": [1,2,3]
        }
        response = requests.post(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case6 error url
        removeParams = {
            "dbName": "db100",
            "listName": "list1",
            "listValue": "hello"
        }
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromlist"
        response = requests.post(errorUrl, json=removeParams)
        self.writeLog(errorUrl, json.dumps(removeParams), response.content.decode())

if __name__ == "__main__":
    test = listTest()

    # testing create list function
    #test.createListTest()

    # testing get list function
    #test.getListTest()

    # testing insert list function
    #test.insertListTest()

    # testing delete list function
    #test.deleteListTest()

    # testing remove from list function
    test.rmListTest()