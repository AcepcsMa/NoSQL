__author__ = 'Ma Haoxiang'

# import
import json

class configParser:

    def __init__(self, configPath=None):
        self.configPath = configPath

    # read server configuration
    def getServerConfig(self, confFileName):
        if(self.configPath is not None):
            filePath = self.configPath + confFileName
        else:
            filePath = confFileName

        with open(filePath,"r") as configFile:
            configJson = json.loads(configFile.read())
            configJson['DEBUG'] = True if configJson["DEBUG"] == "TRUE" else False
            return configJson


if __name__ == '__main__':
    pass
