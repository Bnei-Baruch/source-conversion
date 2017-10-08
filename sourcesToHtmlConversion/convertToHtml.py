#!/usr/bin/python3
import fnmatch
import os
import sys
import multiprocessing
import shutil
import json
import argparse
import datetime
import tempfile

#import logging
#from multiprocessing_logging import install_mp_handler # for logging from multiprocessing.pool processes

# DB
import psycopg2

# conversionFunctions.py
import conversionFunctions
# fileUtils.py
from fileUtils import printIterableToFile, normalizeFilePath

def getLangFromFileName(filename):
    fileNameParts = filename.split('_')
    # some new files started wih page number (____beavoda/80_rabash/mekorot/02_igrot/1424_rb-igeret-03/1424_heb_o_rb-igeret-03.docx)
    # this files = well formated, so we prefer to get them
    langIndex = 0
    try:
        int(fileNameParts[langIndex])
        langIndex = 1
    except ValueError:
        langIndex = 0
    lang = fileNameParts[langIndex]
    return (lang, True if langIndex == 1 else False)


def main(args):
    parser = argparse.ArgumentParser(description='Convertion doc -> docx -> cleaned html + mbId -> uid directory structure. Works with a copy of source folder', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('srcFolderPath', action="store", help="Root folder (copy to destfolder and then copied_folder is processed)")
    parser.add_argument('-destFolderPath', action="store", default=".", help="Dest folder")
    parser.add_argument('-langMapFilePath', default="./langMap.json", action="store", help="postgres connection params")
    parser.add_argument('-postgresqlOptFilePath', default="./postgresqlOpt.json", action="store", help="postgres connection params as json")
    parser.add_argument('-tidyOptionsFilePath', default="./tidyOptions.json", action="store", help="options for tidy (html -> cleaned html) as json")
    results = parser.parse_args(args)

    srcFolderPath = normalizeFilePath(results.srcFolderPath)
    destFolderPath = normalizeFilePath(results.destFolderPath)
    langMapFilePath = normalizeFilePath(results.langMapFilePath)
    postgresqlOptFilePath = normalizeFilePath(results.postgresqlOptFilePath)
    tidyOptionsFilePath = normalizeFilePath(results.tidyOptionsFilePath)

    # if we need rename result files with special suffixes like '_pandoc.html' etc
    indexJsonFileName = "index.json"

    # for future logging usage
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [p:(%(processName)s)] %(levelname)s - %(message)s')
    # install_mp_handler()

    with open(langMapFilePath) as langMap_json:
        langMap = json.load(langMap_json)


    with open(tidyOptionsFilePath) as tidyOptions_json:
        tidyOptions = json.load(tidyOptions_json)

    # Connect to mdb
    with open(postgresqlOptFilePath) as postgresqlOpt_json:
        postgresqlOpt = json.load(postgresqlOpt_json)
    connection = psycopg2.connect(host=postgresqlOpt["host"], dbname=postgresqlOpt["dbname"], user=postgresqlOpt["user"], password=postgresqlOpt["password"])
    # Open a cursor to perform database operations
    cur = connection.cursor()
    cur.execute("select id, uid from sources;")
    idUidMap = dict(cur.fetchall())


    #result of analisys folder
    convertingSummaryFolder = os.path.join(destFolderPath, 'convertingSummary')
    os.makedirs(convertingSummaryFolder, exist_ok=True)

    # working on copy (!!!clear previous one!!!)
    convertedFolder = os.path.join(destFolderPath, 'converted')
    if (os.path.exists(convertedFolder)):
        print("\tDelete previous working folder...")
        shutil.rmtree(convertedFolder)
    print("\tCopy organized sources to working folder...")
    shutil.copytree(srcFolderPath, convertedFolder)
    srcFolderPath = convertedFolder


    # soffice does not allow  more than 1 conversion at time (otherwise it hangs)
    def setConvertToDocxProcessName(name):
        multiprocessing.current_process().name = name
        return

    sofficepool = multiprocessing.Pool(1, initializer=setConvertToDocxProcessName, initargs=["soffice"])

    # pandoc and tidy allow parallel conversions
    procCount = 1
    def setConvertToHtmlProcessName(name, procCount):
        multiprocessing.current_process().name = name + str(procCount)
        procCount += 1
        return

    pandocpool = multiprocessing.Pool(multiprocessing.cpu_count(), initializer=setConvertToHtmlProcessName, initargs=["pandoc_with_tidy", procCount])

    convertFromDocToDocxBatchPaths = []
    convertFromDocxToHtmlTasks = []
    convertedDocToDocxPathsCounter = 0
    convertedDocxToHtmlCounter = 0
    fileNamesWithUnrecognizedLangs = []
    unrecognizedFolderNames = []
    sourcesCounter = 0
    allConvertedToDocsDocs = []

    tidyErrors = multiprocessing.Manager().Queue()
    sofficeErrors = multiprocessing.Manager().Queue()

    with tempfile.TemporaryDirectory() as tmpdirname:
        print('\tCreate temp directory {}'.format(tmpdirname))

        allSourcesFolders = os.listdir(srcFolderPath)
        directoriesToProcessBatchSize = 50 # ~150 directories at once

        convertingDocFileNameToDirName = {}
        outerRangeUpBorder = len(allSourcesFolders) // directoriesToProcessBatchSize
        outerRangeUpBorder = outerRangeUpBorder if (len(allSourcesFolders) % directoriesToProcessBatchSize == 0) else outerRangeUpBorder + 1
        # range(INCLUSIVE, EXCLUSIVE)
        for i in range(0, outerRangeUpBorder):
            # convertion doc -> docx
            innerRangeDownBorder = i*directoriesToProcessBatchSize
            innerRangeUpBorder = min(len(allSourcesFolders), (i+1)*directoriesToProcessBatchSize)

            for j in range(innerRangeDownBorder, innerRangeUpBorder):
                dirName = allSourcesFolders[j]
                dirPath = normalizeFilePath(os.path.join(srcFolderPath, dirName))
                convertDocsInPath = False
                if (os.path.isdir(dirPath)):  # self check
                    sourcesCounter += 1
                    print("\tConverting #{}({}) at folder (mbId='{}')".format(sourcesCounter, len(allSourcesFolders), dirName))
                    for filename in fnmatch.filter(os.listdir(dirPath), "*.doc"):
                        convertDocsInPath = True
                        if(filename in (convertingDocFileNameToDirName.keys())):
                            tmpFileName = dirPath + os.sep + 'TMP_NAME_' + dirName + '_' + filename
                            os.rename(dirPath + os.sep + filename, tmpFileName)
                            convertingDocFileNameToDirName[os.path.splitext(tmpFileName)[0]] = dirPath
                        else:
                            convertingDocFileNameToDirName[os.path.splitext(filename)[0]] = dirPath
                        allConvertedToDocsDocs.append((dirPath + os.path.sep + filename))
                    if(convertDocsInPath):
                        # convertFromDocToDocx(workingPaths)
                        convertFromDocToDocxBatchPaths.append(dirPath)

            if (len(convertFromDocToDocxBatchPaths) > 0):
                # convertFromDocToDocx(inputPaths, outputPath, filenameToFolderMap, sofficeErrors)
                sofficepool.apply_async(conversionFunctions.convertFromDocToDocx, (convertFromDocToDocxBatchPaths, tmpdirname, convertingDocFileNameToDirName, sofficeErrors)).get()
                convertedDocToDocxPathsCounter += len(convertFromDocToDocxBatchPaths)
                convertFromDocToDocxBatchPaths.clear()
                convertingDocFileNameToDirName.clear()

            # convertion docx -> html -> cleaned html
            for j in range(innerRangeDownBorder, innerRangeUpBorder):
                dirName = allSourcesFolders[j]
                dirPath = normalizeFilePath(os.path.join(srcFolderPath, dirName))
                for filename in fnmatch.filter(os.listdir(dirPath), "*.docx"):
                    if (os.path.exists(os.path.join(dirPath, os.path.splitext(filename)[0] + ".html"))):
                        continue
                    else:
                        # convertFromDocxToHtml(sourcepath, destpath, tidyOpt, tidyErrors)
                        convertFromDocxToHtmlTasks.append((os.path.join(dirPath, filename), os.path.join(dirPath, os.path.splitext(filename)[0] + ".html"), tidyOptions, tidyErrors))

            if (len(convertFromDocxToHtmlTasks) > 0):
                results = [pandocpool.apply_async(conversionFunctions.convertFromDocxToHtml, t) for t in convertFromDocxToHtmlTasks]
                for result in results:
                    result.get()
                convertedDocxToHtmlCounter += len(convertFromDocxToHtmlTasks)
                convertFromDocxToHtmlTasks.clear()

            # create index.json with lang->doc map
            for j in range(innerRangeDownBorder, innerRangeUpBorder):
                dirName = allSourcesFolders[j]
                dirPath = normalizeFilePath(os.path.join(srcFolderPath, dirName))
                langToDocsMap = {}
                for filename in os.listdir(dirPath):
                    # return (lang, newDocFormat)
                    lang2letter = langMap.get(getLangFromFileName(filename)[0].upper(), False)
                    if (not lang2letter):
                        print("\t\tSkip filename with unrecognized lang: {}".format(filename))
                        fileNamesWithUnrecognizedLangs.append((dirPath + os.path.sep + filename))
                        continue
                    if (lang2letter not in langToDocsMap):
                        langToDocsMap[lang2letter] = {}
                    langToDocsMap[lang2letter][os.path.splitext(filename)[1][1:]] = filename
                indexjson = json.dumps(langToDocsMap, indent=4)
                with open(dirPath + os.path.sep + indexJsonFileName, 'w') as myfile:
                    myfile.write(indexjson)

                # rename folders
                try:
                    mbId = int(dirName)
                except ValueError as err:
                    print("\t\tUnrecognized mbId: {}".format(dirName))
                    unrecognizedFolderNames.append(dirName)
                    continue

                uid = idUidMap.get(mbId)
                if (not uid):
                    print("\t\tUnrecognized mbId: {}".format(dirName))
                    unrecognizedFolderNames.append(mbId)
                else:
                    print("\t\tRename {}\n\t\t\tto {}".format(srcFolderPath + os.path.sep + dirName, srcFolderPath + os.path.sep + uid))
                    os.rename(srcFolderPath + os.path.sep + dirName, srcFolderPath + os.path.sep + uid)

        currentDate = str(datetime.datetime.now()).replace(' ', '_');

        print("\ttmpFolder: {}, Batch size: {}\nNum of doc->docx (soffice) folders conversioned: {}\nNum of docx->html (pandoc + tidy) conversions: {}".format(tmpdirname, directoriesToProcessBatchSize, convertedDocToDocxPathsCounter, convertedDocxToHtmlCounter))
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'allConvertedDocs_{}.txt'.format(currentDate)), allConvertedToDocsDocs)
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'fileNamesWithUnrecognizedLangs_{}.txt'.format(currentDate)), fileNamesWithUnrecognizedLangs)
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'unrecognizedFolderNames_{}.txt'.format(currentDate)), unrecognizedFolderNames)
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'idUidMap_{}.txt'.format(currentDate)), idUidMap)
        #to end "iter" call below
        tidyErrors.put(None)
        sofficeErrors.put(None)

        def multiprocessingQueueToList(queue):
            listFromQueue = []
            for i in iter(queue.get, None):
                listFromQueue.append(i)
            return listFromQueue

        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'tidyErrors_{}.txt'.format(currentDate)), [("workingPath", "errors")] + multiprocessingQueueToList(tidyErrors))
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'sofficeErrors_{}.txt'.format(currentDate)), [("cmd", "workingPath", "errors")] + multiprocessingQueueToList(sofficeErrors))



if __name__ == "__main__":
    main(sys.argv[1:])
