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
        url = "http://" + self.host + ":" + str(self.port) + "/makeList"

        params = {
            "dbName": "db0",
            "listName": "list1"
        }

        # case1 create a list
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params),response.content.decode())

        # case2 create a list repeatedly
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case3 unknown database name
        params["dbName"] = "db100"
        response = requests.post(url=url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/makelist"
        response = requests.get(url=errorUrl, json=params)
        self.writeLog(errorUrl, json.dumps(params),response.content.decode())

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

    def leftGetListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/leftGetList/{0}/{1}/{2}"

        # case1 create a list and get its value
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        params = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(url=createUrl, json=params)
        response = requests.get(url.format("db0", "list1", 3))
        self.writeLog(url.format("db0", "list1", 3), "", response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db999", "list1", 3))
        self.writeLog(url.format("db999", "list1", 3), "", response.content.decode())

        # case3 unknown list name
        response = requests.get(url.format("db0", "list999", 3))
        self.writeLog(url.format("db0", "list999", 3), "", response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/leftgetlist/{0}/{1}"
        response = requests.get(errorUrl.format("db0", "list1", 3))
        self.writeLog(errorUrl.format("db0", "list1", 3), "", response.content.decode())

    def rightGetListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rightGetList/{0}/{1}/{2}"

        # case1 create a list and get its value
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        params = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(url=createUrl, json=params)
        response = requests.get(url.format("db0", "list1", 3))
        self.writeLog(url.format("db0", "list1", 3), "", response.content.decode())

        # case2 unknown database name
        response = requests.get(url.format("db999", "list1", 3))
        self.writeLog(url.format("db999", "list1", 3), "", response.content.decode())

        # case3 unknown list name
        response = requests.get(url.format("db0", "list999", 3))
        self.writeLog(url.format("db0", "list999", 3), "", response.content.decode())

        # case4 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rightgetlist/{0}/{1}"
        response = requests.get(errorUrl.format("db0", "list1", 3))
        self.writeLog(errorUrl.format("db0", "list1", 3), "", response.content.decode())

    def insertListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/insertList"

        # case1 create a list and then insert
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        createParams = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(createUrl, json=createParams)
        params = {
            "dbName":"db0",
            "listName":"list1",
            "listValue":123
        }
        response = requests.put(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case2 insert repeatedly
        response = requests.put(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case3 unknown database name
        errorParams = {
            "dbName": "db100",
            "listName": "list1",
            "listValue": 123
        }
        response = requests.put(url, json=errorParams)
        self.writeLog(url, json.dumps(errorParams), response.content.decode())

        # case4 unknown list name
        errorParams = {
            "dbName": "db0",
            "listName": "list999",
            "listValue": 123
        }
        response = requests.put(url, json=errorParams)
        self.writeLog(url, json.dumps(errorParams), response.content.decode())

        # case5 different data type
        params = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": [1,2,"hello"]
        }
        response = requests.put(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case6 error database type
        errorParams = {
            "dbName": [1,2,3],
            "listName": "list999",
            "listValue": 123
        }
        response = requests.put(url, json=errorParams)
        self.writeLog(url, json.dumps(errorParams), response.content.decode())

        # case7 error list type
        errorParams = {
            "dbName": "db0",
            "listName": [4,5,6],
            "listValue": 123
        }
        response = requests.put(url, json=errorParams)
        self.writeLog(url, json.dumps(errorParams), response.content.decode())

        # case8 error url
        errorParams = {
            "dbName": "db0",
            "listName": "list999",
            "listValue": 123
        }
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/insertlist"
        response = requests.put(errorUrl, json=errorParams)
        self.writeLog(errorUrl, json.dumps(errorParams), response.content.decode())

    def leftInsertTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/leftInsertList"

        # case1 create a list and then insert
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        createParams = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(createUrl, json=createParams)
        params = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": 123
        }
        response = requests.put(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case2 insert repeatedly
        response = requests.put(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case3 unknown database name
        errorParams = {
            "dbName": "db100",
            "listName": "list1",
            "listValue": 123
        }
        response = requests.put(url, json=errorParams)
        self.writeLog(url, json.dumps(errorParams), response.content.decode())

        # case4 unknown list name
        errorParams = {
            "dbName": "db0",
            "listName": "list999",
            "listValue": 123
        }
        response = requests.put(url, json=errorParams)
        self.writeLog(url, json.dumps(errorParams), response.content.decode())

        # case5 different data type
        params = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": [1, 2, "hello"]
        }
        response = requests.put(url, json=params)
        self.writeLog(url, json.dumps(params), response.content.decode())

        # case6 error database type
        errorParams = {
            "dbName": [1, 2, 3],
            "listName": "list999",
            "listValue": 123
        }
        response = requests.put(url, json=errorParams)
        self.writeLog(url, json.dumps(errorParams), response.content.decode())

        # case7 error list type
        errorParams = {
            "dbName": "db0",
            "listName": [4, 5, 6],
            "listValue": 123
        }
        response = requests.put(url, json=errorParams)
        self.writeLog(url, json.dumps(errorParams), response.content.decode())

        # case8 error url
        errorParams = {
            "dbName": "db0",
            "listName": "list999",
            "listValue": 123
        }
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/insertlist"
        response = requests.put(errorUrl, json=errorParams)
        self.writeLog(errorUrl, json.dumps(errorParams), response.content.decode())

    def deleteListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/deleteList/{0}/{1}"

        # case1 create a list and then delete
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        createParams = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(createUrl, json=createParams)
        response = requests.delete(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"), "", response.content.decode())

        # case2 delete repeatedly
        response = requests.delete(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"), "", response.content.decode())

        # case3 unknown database name
        response = requests.delete(url.format("db100","list1"))
        self.writeLog(url.format("db100","list1"), "", response.content.decode())

        # case4 unknown list name
        response = requests.delete(url.format("db0","list111"))
        self.writeLog(url.format("db0","list111"), "", response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/deletelist/{0}/{1}"
        response = requests.delete(errorUrl.format("db0","list1"))
        self.writeLog(errorUrl.format("db0","list1"), "", response.content.decode())

    def rmListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/rmFromList"

        # case1 create a list, insert some values and then remove
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        createParams = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(createUrl, json=createParams)

        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertList"
        params = {
            "dbName":"db0",
            "listName":"list1",
            "listValue":"hello"
        }
        response = requests.put(insertUrl, json=params)
        removeParams = params
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case2 remove repeatedly
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case3 unknown database name
        removeParams = {
            "dbName":"db100",
            "listName":"list1",
            "listValue":"hello"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case4 unknown list name
        removeParams = {
            "dbName": "db0",
            "listName": "list111",
            "listValue": "hello"
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case5 non-existed value
        removeParams = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": [1,2,3]
        }
        response = requests.put(url, json=removeParams)
        self.writeLog(url, json.dumps(removeParams), response.content.decode())

        # case6 error url
        removeParams = {
            "dbName": "db100",
            "listName": "list1",
            "listValue": "hello"
        }
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/rmfromlist"
        response = requests.put(errorUrl, json=removeParams)
        self.writeLog(errorUrl, json.dumps(removeParams), response.content.decode())

    def clearListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearList/{0}/{1}"

        # case1 create a list, insert some values and then clear
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        createParams = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(createUrl, json=createParams)

        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertList"
        params = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": "hello"
        }
        response = requests.put(insertUrl, json=params)
        response = requests.put(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case2 clear repeatedly
        response = requests.put(url.format("db0","list1"))
        self.writeLog(url.format("db0","list1"),"",response.content.decode())

        # case3 unknown database name
        response = requests.put(url.format("db999","list1"))
        self.writeLog(url.format("db999","list1"),"",response.content.decode())

        # case4 unknown list name
        response = requests.put(url.format("db0","list999"))
        self.writeLog(url.format("db0","list999"),"",response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearlist/{0}/{1}"

        response = requests.put(errorUrl.format("db0","list1"))
        self.writeLog(errorUrl.format("db0","list1"),"",response.content.decode())

    def mergeListTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/mergeLists"

        # case1 create two lists and then merge into a third list
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        createParams = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(createUrl, json=createParams)
        createParams["listName"] = "list2"
        response = requests.post(createUrl, json=createParams)

        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertList"
        params = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": "hello"
        }
        response = requests.put(insertUrl, json=params)
        params["listName"] = "list2"
        params["listValue"] = 123
        response = requests.put(insertUrl, json=params)

        mergeParams = {
            "dbName":"db0",
            "list1":"list1",
            "list2":"list2",
            "resultList":"mergeResult"
        }
        response = requests.put(url,json=mergeParams)
        self.writeLog(url, json.dumps(mergeParams), response.content.decode())

        # case2 merge result list already exists
        mergeParams = {
            "dbName":"db0",
            "list1":"list1",
            "list2":"list2",
            "resultList":"mergeResult"
        }
        response = requests.put(url,json=mergeParams)
        self.writeLog(url, json.dumps(mergeParams), response.content.decode())

        # case3 merge into the first list
        mergeParams = {
            "dbName":"db0",
            "list1":"list1",
            "list2":"list2",
            "resultList":""
        }
        response = requests.put(url,json=mergeParams)
        self.writeLog(url, json.dumps(mergeParams), response.content.decode())

        # case4 unknown database name
        mergeParams = {
            "dbName":"db999",
            "list1":"list1",
            "list2":"list2",
            "resultList":"mergeResult"
        }
        response = requests.put(url,json=mergeParams)
        self.writeLog(url, json.dumps(mergeParams), response.content.decode())

        # case5 unknown list name
        mergeParams = {
            "dbName":"db0",
            "list1":"list123",
            "list2":"list456",
            "resultList":"mergeResult"
        }
        response = requests.put(url,json=mergeParams)
        self.writeLog(url, json.dumps(mergeParams), response.content.decode())

        # case6 error database name type
        mergeParams = {
            "dbName":[1,2,3,4,5],
            "list1":"list123",
            "list2":"list456",
            "resultList":"mergeResult"
        }
        response = requests.put(url,json=mergeParams)
        self.writeLog(url, json.dumps(mergeParams), response.content.decode())

        # case7 error list name type
        mergeParams = {
            "dbName":"db0",
            "list1":["a","b","c"],
            "list2":[1,2,3],
            "resultList":[4,5,6]
        }
        response = requests.put(url,json=mergeParams)
        self.writeLog(url, json.dumps(mergeParams), response.content.decode())

        # case8 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/mergelists"
        response = requests.put(errorUrl,json=mergeParams)
        self.writeLog(errorUrl, json.dumps(mergeParams), response.content.decode())

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
        url = "http://" + self.host + ":" + str(self.port) + "/setTTL"

        # case1 create a list and set TTL
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        createParams = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(url=createUrl, json=createParams)

        ttlParams = {
            "dataType": "LIST",
            "dbName": "db0",
            "keyName": "list1",
            "ttl": 20
        }
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case2 unknown database name
        ttlParams["dbName"] = "db123"
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case3 unknown list name
        ttlParams["dbName"] = "db0"
        ttlParams["keyName"] = "list123"
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case4 TTL is not Int type
        ttlParams["keyName"] = "list1"
        ttlParams["ttl"] = "abc"
        response = requests.post(url=url, json=ttlParams)
        self.writeLog(url, json.dumps(ttlParams), response.content.decode())

        # case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/setttl"
        response = requests.post(url=errorUrl, json=ttlParams)
        self.writeLog(errorUrl, json.dumps(ttlParams), response.content.decode())

    # test clear TTL function
    def clearTTLTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/clearTTL"

        # case1 set a TTL and then clear it
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList"
        createParams = {
            "dbName": "db0",
            "listName": "list1"
        }
        response = requests.post(url=createUrl, json=createParams)

        setUrl = "http://" + self.host + ":" + str(self.port) + "/setTTL"
        ttlParams = {
            "dataType": "LIST",
            "dbName": "db0",
            "keyName": "list1",
            "ttl": 20
        }
        response = requests.post(url=setUrl, json=ttlParams)

        clearParams = {
            "dataType": "LIST",
            "dbName": "db0",
            "keyName": "list1"
        }
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case2 clear TTL repeatedly
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case3 clear non-existed TTL
        createParams = {
            "dbName": "db0",
            "listName": "list2"
        }
        response = requests.post(url=createUrl, json=createParams)
        clearParams["keyName"] = "list2"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case4 unknown database name
        clearParams["dbName"] = "db999"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case5 unknown element name
        clearParams["dbName"] = "db0"
        clearParams["keyName"] = "list123"
        response = requests.post(url=url, json=clearParams)
        self.writeLog(url, json.dumps(clearParams), response.content.decode())

        # case6 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/clearttl"
        response = requests.get(url=errorUrl, json=clearParams)
        self.writeLog(errorUrl, json.dumps(clearParams), response.content.decode())

    def getSizeTest(self):
        url = "http://" + self.host + ":" + str(self.port) + "/getListSize/{}/{}"

        # case1 create a list, insert, and get its size
        createUrl = "http://" + self.host + ":" + str(self.port) + "/makeList/{}/{}"
        insertUrl = "http://" + self.host + ":" + str(self.port) + "/insertList"
        response = requests.get(createUrl.format("db0", "list1"))
        insertParmas = {
            "dbName": "db0",
            "listName": "list1",
            "listValue": "123"
        }
        response = requests.post(insertUrl, json=insertParmas)
        response = requests.get(url.format("db0", "list1"))
        self.writeLog(url.format("db0", "list1"), "", response.content.decode())

        # case2 get size of an empty list
        response = requests.get(createUrl.format("db0", "list2"))
        response = requests.get(url.format("db0", "list2"))
        self.writeLog(url.format("db0", "list2"), "", response.content.decode())

        # case3 unknown database name
        response = requests.get(url.format("db123", "list1"))
        self.writeLog(url.format("db123", "list1"), "", response.content.decode())

        # case4 unknown hash name
        response = requests.get(url.format("db0", "list123"))
        self.writeLog(url.format("db0", "list123"), "", response.content.decode())

        #case5 error url
        errorUrl = "http://" + self.host + ":" + str(self.port) + "/getlistsize/{}/{}"
        response = requests.get(errorUrl.format("db0", "list1"))
        self.writeLog(errorUrl.format("db0", "list1"), "", response.content.decode())

if __name__ == "__main__":
    test = listTest()

    # testing create list function
    # test.createListTest()

    # testing get list function
    # test.getListTest()

    # testing insert list function
    # test.insertListTest()

    # testing left insert list function
    # test.leftInsertTest()

    # testing delete list function
    # test.deleteListTest()

    # testing remove from list function
    # test.rmListTest()

    # testing clear list function
    # test.clearListTest()

    # testing merge function
    # test.mergeListTest()

    # testing search function
    #test.searchListTest()

    # testing set TTL function
    # test.setTTLTest()

    # testing clear TTL function
    # test.clearTTLTest()

    # testing get size function
    # test.getSizeTest()

    # testing left get function
    # test.leftGetListTest()

    # testing right get function
    # test.rightGetListTest()