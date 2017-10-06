#!/usr/bin/python3
import fnmatch
import os
import sys
import multiprocessing
import shutil
import json
import argparse
import datetime

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
    if os.path.exists(convertedFolder):
        shutil.rmtree(convertedFolder)
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

    convertFromDocToDocxTasks = []
    convertFromDocxToHtmlTasks = []
    convertedDocToDocxCounter = 0
    convertedDocxToHtmlCounter = 0
    fileNamesWithUnrecognizedLangs = []
    unrecognizedFolderNames = []
    sourcesCounter = 0

    tidyErrors = []
    sofficeErrors = []

    try:
        allSourcesFolders = os.listdir(srcFolderPath)
        for dirName in allSourcesFolders:
            workingPath = normalizeFilePath(os.path.join(srcFolderPath, dirName))
            if (os.path.isdir(workingPath)):  # self check
                sourcesCounter += 1
                print("Converting #{}(#{}) folder (mbId='{}')".format(sourcesCounter, len(allSourcesFolders), dirName))
                for filename in fnmatch.filter(os.listdir(workingPath), "*.doc"):
                    if (os.path.exists(os.path.join(workingPath, os.path.splitext(filename)[0] + '.docx'))):
                        continue
                    else:
                        # convertFromDocToDocx(workingPath, errors)
                        convertFromDocToDocxTasks.append((workingPath, sofficeErrors))
                        break;
                if (len(convertFromDocToDocxTasks) > 0):
                    results = [sofficepool.apply_async(conversionFunctions.convertFromDocToDocx, t) for t in convertFromDocToDocxTasks]
                    for result in results:
                        result.get()
                        convertedDocToDocxCounter += 1
                    convertFromDocToDocxTasks.clear()

                for filename in fnmatch.filter(os.listdir(workingPath), "*.docx"):
                    if (os.path.exists(os.path.join(workingPath, os.path.splitext(filename)[0] + ".html"))):
                        continue
                    else:
                        # convertFromDocxToHtml(sourcepath, destpath, tidyOpt, tidyErrors)
                        convertFromDocxToHtmlTasks.append((os.path.join(workingPath, filename), os.path.join(workingPath, os.path.splitext(filename)[0] + ".html"), tidyOptions, tidyErrors))

                if (len(convertFromDocxToHtmlTasks) > 0):
                    results = [pandocpool.apply_async(conversionFunctions.convertFromDocxToHtml, t) for t in convertFromDocxToHtmlTasks]
                    for result in results:
                        result.get()
                        convertedDocxToHtmlCounter += 1
                    convertFromDocxToHtmlTasks.clear()

                # create index.json with lang->doc map
                langToDocsMap = {}
                for filename in os.listdir(workingPath):
                    # return (lang, newDocFormat)
                    lang2letter = langMap.get(getLangFromFileName(filename)[0].upper(), False)
                    if (not lang2letter):
                        print("Skip filename with unrecognized lang: {}".format(filename))
                        fileNamesWithUnrecognizedLangs.append((workingPath + os.path.sep + filename))
                        continue
                    if (lang2letter not in langToDocsMap):
                        langToDocsMap[lang2letter] = {}
                    langToDocsMap[lang2letter][os.path.splitext(filename)[1][1:]] = filename
                indexjson = json.dumps(langToDocsMap, indent=4)
                with open(workingPath + os.path.sep + indexJsonFileName, 'w') as myfile:
                    myfile.write(indexjson)

                # rename folders
                try:
                    mbId = int(dirName)
                except ValueError as err:
                    print("Unrecognized mbId: {}".format(dirName))
                    unrecognizedFolderNames.append(dirName)
                    continue

                uid = idUidMap.get(mbId)
                if (not uid):
                    print("Unrecognized mbId: {}".format(dirName))
                    unrecognizedFolderNames.append(mbId)
                else:
                    os.rename(srcFolderPath + os.path.sep + dirName, srcFolderPath + os.path.sep + uid)

        currentDate = str(datetime.datetime.now()).replace(' ', '_');

        print("Num of doc->docx (soffice) conversions: {}\nNum of docx->html (pandoc + tidy) conversions: {}".format(convertedDocToDocxCounter, convertedDocxToHtmlCounter))
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'fileNamesWithUnrecognizedLangs_{}.txt'.format(currentDate)), fileNamesWithUnrecognizedLangs)
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'unrecognizedFolderNames_{}.txt'.format(currentDate)), unrecognizedFolderNames)
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'idUidMap_{}.txt'.format(currentDate)), idUidMap)
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'tidyErrors_{}.txt'.format(currentDate)), tidyErrors)
        printIterableToFile(os.path.join(destFolderPath, convertingSummaryFolder, 'sofficeErrors_{}.txt'.format(currentDate)), sofficeErrors)
    except OSError as err:
        print(err)


if __name__ == "__main__":
    main(sys.argv[1:])
