__author__ = 'Ma Haoxiang'

# import
import flask
import db
from configParser import configParser

app = flask.Flask(__name__)

@app.route("/make/<expression>",methods=["GET"])
def makeElem(expression):
    if("->" not in expression):
        return "Expression Error!"
    else:
        dbName = expression.split("->")[0]
        elemName = expression.split("->")[1]
        elemValue = expression.split("->")[2]
        result = myDb.createElem(elemName, elemValue, dbName)
        if(result == myDb.CREATE_ELEM_SUCCESS):
            return "Make Element Success"
        elif(result == myDb.ELEM_ALREADY_EXIST):
            return "Element Already Exists"
        elif(result == myDb.ELEM_TYPE_ERROR):
            return "Element Type Error"


@app.route("/get/<element>",methods=["GET"])
def getElem(element):
    dbName = element.split("->")[0]
    elemName = element.split("->")[1]
    result = myDb.getElem(elemName, dbName)
    if(result[0] == myDb.ELEM_NOT_EXIST):
        return None
    elif(result[0] == myDb.GET_ELEM_SUCCESS):
        return result[1]
    elif(result[0] == myDb.ELEM_TYPE_ERROR):
        return None


@app.route("/searchElem/<dbName>/<string:expression>",methods=["GET"])
def searchElem(dbName,expression):
    result = myDb.searchElem(expression, dbName)
    return flask.jsonify(result)


@app.route("/getAllElem/<dbName>",methods=["GET"])
def getAllElem(dbName):
    return flask.jsonify(myDb.getAllElem(dbName))



if __name__ == '__main__':

    # init the database
    myDb = db.NoSqlDb()

    # init the config parser and read the server config
    confParser = configParser()
    serverConfig = confParser.getServerConfig("server.conf")

    # run the server
    app.run(host=serverConfig["HOST"],port=serverConfig["PORT"],debug=serverConfig["DEBUG"])