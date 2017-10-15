import os
import sys
import hashlib
import json

# python bad works with path which has mixed backslashes
# for example ('/run/media/dalegur/DATA/Documents/BB_MDB/Mapping/docs_copy/____beavoda\\70_baal-sulam\\mekorot\\akdamot\\akdama-pticha'
# will not be recognized as valid path  (so path will no be found)
def normalizeFilePath(filePath):
    if (filePath == ""):
        return filePath
    return os.path.abspath(filePath).replace("\\", "/")


#Class for print to stdout and to file simultaneously
class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush() # If you want the output to be visible immediately
    def flush(self) :
        for f in self.files:
            f.flush()

#functon to print both console and file
def printToFileAndConsole(file, stringstoprint):
    original = sys.stdout
    try:
        sys.stdout = Tee(sys.stdout, file)
        for stringtoprint in stringstoprint:
            print(stringtoprint)
    finally:
        sys.stdout = original
    return

def printIterableToFile(filepath, iterable):
    "Every entry at new line"
    with open(filepath, 'w', encoding='UTF8') as currentFile:
        currentFile.write(json.dumps(iterable, indent=4))
    return

