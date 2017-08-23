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

    def clearListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearList/{0}/{1}"

        # case1 create a list, insert some values and then clear
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertList"

        response = requests.get(createUrl.format("db0", "list1"))
        params = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": "hello"
        }
        response = requests.post(insertUrl, json=params)
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case2 clear repeatedly
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db999","list1"))
        self.writeLog(url.format("db999","list1"),"",response.content.decode())

        # case4 unknown list name
        response = requests.get(url.format("db0","list999"))
        self.writeLog(url.format("db0","list999"),"",response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearlist/{0}/{1}"

        response = requests.get(errorUrl.format("db0","list1"))
        self.writeLog(errorUrl.format("db0","list1"),"",response.content.decode())

    def mergeListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/mergeLists"

        # case1 create two lists and then merge into a third list
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertList"

        response = requests.get(createUrl.format("db0", "list1"))
        response = requests.get(createUrl.format("db0", "list2"))
        params = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": "hello"
        }
        response = requests.post(insertUrl, json=params)
        params["listName"] = "list2"
        params["listValue"] = 123
        response = requests.post(insertUrl, json=params)

        mergeParams = {
            "dbName":"db0",
            "list1":"list1",
            "list2":"list2",
            "resultList":"mergeResult"
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case2 merge result list already exists
        mergeParams = {
            "dbName":"db0",
            "list1":"list1",
            "list2":"list2",
            "resultList":"mergeResult"
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case3 merge into the first list
        mergeParams = {
            "dbName":"db0",
            "list1":"list1",
            "list2":"list2",
            "resultList":""
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case4 unknown database name
        mergeParams = {
            "dbName":"db999",
            "list1":"list1",
            "list2":"list2",
            "resultList":"mergeResult"
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case5 unknown list name
        mergeParams = {
            "dbName":"db0",
            "list1":"list123",
            "list2":"list456",
            "resultList":"mergeResult"
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case6 error database name type
        mergeParams = {
            "dbName":[1,2,3,4,5],
            "list1":"list123",
            "list2":"list456",
            "resultList":"mergeResult"
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case7 error list name type
        mergeParams = {
            "dbName":"db0",
            "list1":["a","b","c"],
            "list2":[1,2,3],
            "resultList":[4,5,6]
        }
        response = requests.post(url,json=mergeParams)
        self.writeLog(url,json.dumps(mergeParams),response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/mergelists"
        response = requests.post(errorUrl,json=mergeParams)
        self.writeLog(errorUrl,json.dumps(mergeParams),response.content.decode())

    def searchListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/searchList/{0}/{1}"

        # case1 create several lists and then search by a*
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        response = requests.get(createUrl.format("db0","ablist"))
        response = requests.get(createUrl.format("db0","a"))
        response = requests.get(createUrl.format("db0","ab123"))
        response = requests.get(createUrl.format("db0","bbalist"))
        response = requests.get(createUrl.format("db0","bacg"))

        response = requests.get(url.format("db0","a*"))
        self.writeLog(url.format("db0","a*"),"",response.content.decode())

        # case2 search by ab*
        response = requests.get(url.format("db0","ab*"))
        self.writeLog(url.format("db0","ab*"),"",response.content.decode())

        # case3 search by b*
        response = requests.get(url.format("db0","b*"))
        self.writeLog(url.format("db0","b*"),"",response.content.decode())

        # case4 search by ab
        response = requests.get(url.format("db0","ab"))
        self.writeLog(url.format("db0","ab"),"",response.content.decode())

        # case5 search by a non-existed expression
        response = requests.get(url.format("db0","12345"))
        self.writeLog(url.format("db0","12345"),"",response.content.decode())

        # case6 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/searchlist/{0}/{1}"
        response = requests.get(errorUrl.format("db0","a*"))
        self.writeLog(errorUrl.format("db0","a*"),"",response.content.decode())

    def setTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/setListTTL/{0}/{1}/{2}"

        # case1 create a list and set TTL
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        response = requests.get(createUrl.format("db0","list1"))
        response = requests.get(url.format("db0","list1",20))
        self.writeLog(url.format("db0","list1",20),"",response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db123","list1",15))
        self.writeLog(url.format("db123","list1",15),"",response.content.decode())

        # case3 unknown list name
        response = requests.get(url.format("db0","list123",15))
        self.writeLog(url.format("db0","list123",15),"",response.content.decode())

        # case4 TTL is not Int type
        response = requests.get(url.format("db0","list1","hi"))
        self.writeLog(url.format("db0","list1","hi"),"",response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/setlistTTL/{0}/{1}/{2}"
        response = requests.get(errorUrl.format("db0", "list1",15))
        self.writeLog(errorUrl.format("db0","list1",15),"",response.content.decode())

    # test clear TTL function
    def clearTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearListTTL/{0}/{1}"

        # case1 set a TTL and then clear it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{0}/{1}"
        setUrl = "http://" + self.host + ":" + str(self.port) + "/setListTTL/{0}/{1}/{2}"
        response = requests.get(createUrl.format("db0","list1"))
        response = requests.get(setUrl.format("db0","list1",20))
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case2 clear TTL repeatedly
        response = requests.get(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case3 clear non-existed TTL
        response = requests.get(createUrl.format("db0","list2"))
        response = requests.get(url.format("db0","list2"))
        self.writeLog(url.format("db0","list2"),"",response.content.decode())

        # case4 unknown database name
        response = requests.get(url.format("db999","list1"))
        self.writeLog(url.format("db999","list1"),"",response.content.decode())

        # case5 unknown element name
        response = requests.get(url.format("db0","list123"))
        self.writeLog(url.format("db0","list123"),"",response.content.decode())

        # case6 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearlistTTL/{0}/{1}"
        response = requests.get(errorUrl.format("db0", "list1"))
        self.writeLog(errorUrl.format("db0","list1"),"",response.content.decode())



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
    #test.rmListTest()

    # testing clear list function
    #test.clearListTest()

    # testing merge function
    #test.mergeListTest()

    # testing search function
    #test.searchListTest()

    # testing set TTL function
    #test.setTTLTest()

    # testing clear TTL function
    test.clearTTLTest()