#!/usr/bin/python3

import sys
import os


import json
import psycopg2
import pickle
import datetime
import json

# compareIndexAndMdb.py
from compareIndexAndMdb import compareIndexPartAndMdb

#import argparse


# fileUtils.py
from fileUtils import normalizeFilePath, printToFileAndConsole




def main(args):
    # parser = argparse.ArgumentParser(description='Run whole pipeline (copy, convert, organize)', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    #
    # #in
    # parser.add_argument('srcFolderPath', action="store", help="Sources folder (PATH/____beavoda/")
    # parser.add_argument('-destFolderPath', action="store", default=".", help="Dest path (all stages folders will be here)")
    #
    # #copy stage
    # parser.add_argument('--skipCopy', action="store_true", default=False, help="Skip copy stage")
    # parser.add_argument('-copyPathPrefix', action="store", default="____beavoda", help="Paths at mapping xlsx file has ____beavoda/ prefix (4.10.2017)")
    # parser.add_argument('-copyPatterns', action="append", default=['*.doc', '*.docx'], help="Patterns for source docs copy stage filtering")
    #
    # #organize stage
    # parser.add_argument('--skipOrganize', action="store_true", default=False, help="Skip organize stage")
    # parser.add_argument('-mappingFilePath', action="store", default="./mapping.xlsx", help="Path to Roza's maping file")
    # parser.add_argument('-organizePatterns', action="append", default=['*.doc', '*.docx'], help="Patterns for files filter")
    # parser.add_argument('-langMapFilePath', action="store", default="./langMap.json", help="Map lang3letters -> lang2letters at JSON format")
    #
    # #converting stage
    # parser.add_argument('--skipConvert', action="store_true", default=False, help="Skip convert stage")
    # parser.add_argument('-postgresqlOptFilePath', action="store", default="./postgresqlOpt.json", help="postgres connection params as json")
    # parser.add_argument('-tidyOptionsFilePath', action="store", default="./tidyOptions.json", help="options for tidy (html -> cleaned html) as json")
    #
    # results = parser.parse_args(args)


    pathToIndexFile = normalizeFilePath("/home/dalegur/Downloads/files-archive-roza_sorted.csv")
    postgresqlOptFilePath = normalizeFilePath("./postgresqlOpt.json")
    destFolderPath = normalizeFilePath('./')
    fetchDataFromDbAndSaveToFile = False
    mdbDFilesFileName = "mdbFilesFromMdb"
    mdbSOurceOCntentUnitRelationFileName = "mdbSourceContentUnitRelation"

    analysisSummaryFolder = os.path.join(destFolderPath, 'analysisSummary')
    databaseDataFolder = os.path.join(destFolderPath, 'databaseDataFolder')
    os.makedirs(analysisSummaryFolder, exist_ok=True)
    os.makedirs(databaseDataFolder, exist_ok=True)

    mdbDFilesFilePath = os.path.join(databaseDataFolder, mdbDFilesFileName)
    mdbSOurceOCntentUnitRelationFilePath = os.path.join(databaseDataFolder, mdbSOurceOCntentUnitRelationFileName)
    # read MDB mdbData
    print("Read MDB Data from {}".format("MDB" if fetchDataFromDbAndSaveToFile else "localFile"))
    if(fetchDataFromDbAndSaveToFile):
        # Connect to mdb
        with open(postgresqlOptFilePath) as postgresqlOpt_json:
            postgresqlOpt = json.load(postgresqlOpt_json)
        connection = psycopg2.connect(host=postgresqlOpt["host"], dbname=postgresqlOpt["dbname"], user=postgresqlOpt["user"], password=postgresqlOpt["password"])
        cur = connection.cursor()
        print("Query files data")
        cur.execute("select encode(f.sha1, 'hex') as sha1, f.id as file_id, f.name as file_name, f.size as file_size, cu.id as content_unit_id, ct.name as content_unit_type_name   from files f left join content_units cu on f.content_unit_id = cu.id left join content_types ct on cu.type_id=ct.id where f.sha1 is not NULL")
        #(sha1, fileId, fileName, fileSize, content_unit_id, content_unit_type_name)
        mdbFilesData = cur.fetchall()

        # for sourceId <-> content_unit mapping
        print("Query sourceId<->contentUnitType data")
        cur.execute("select s.id as sourceId, cu.id as content_unit_id from content_units_sources cus join sources s on cus.source_id = s.id join content_units cu on cus.content_unit_id = cu.id")
        #(sourceId, content_unit_id)
        mdbSourceContentUnitRelationData = cur.fetchall()

        cur.close()
        connection.close()
        print("Save database data as:\n\t'{}'\nand\n\t'{}'".format(mdbDFilesFilePath, mdbSOurceOCntentUnitRelationFilePath))
        with open(mdbDFilesFilePath, 'wb') as mdbFilesFile, open(mdbSOurceOCntentUnitRelationFilePath, 'wb') as mdbSOurceOCntentUnitRelationFile:
            pickle.dump(mdbFilesData, mdbFilesFile)
            pickle.dump(mdbSourceContentUnitRelationData, mdbSOurceOCntentUnitRelationFile)
    else:
        print("Load database data from:\n\t{}\nand\n\t{}".format(mdbDFilesFilePath, mdbSOurceOCntentUnitRelationFilePath))
        with open(mdbDFilesFilePath, 'rb') as mdbFilesFile, open(mdbSOurceOCntentUnitRelationFilePath, 'rb') as mdbSOurceOCntentUnitRelationFile:
            mdbFilesData = pickle.load(mdbFilesFile)
            mdbSourceContentUnitRelationData = pickle.load(mdbSOurceOCntentUnitRelationFile)

    numFilesInIndex = 0

    filesInMdbNotInFs = []
    filesInFsNotInMdb = []

    filesWIthSameHashInMdb = {}
    filesWIthSameHashInFs = {}

    filesWithoutContentUnit = []

    # every file in folder has entry in Mdb (files table) + every file belong one (or two (with KITEI_MEKOV)) content unit
    validFolders = []
    # with description
    invalidFolders = []

    mdbDataMap = {}
    print("Create mapping by hash for {} mdb entries".format(len(mdbFilesData)))
    #   0      1            2        3           4                  5
    # (sha1, fileId, fileName, fileSize, content_unit_id, content_unit_type_name)
    for entry in mdbFilesData:
        (sha1, fileId, fileName, fileSize, contentUnitId, contentTypeName) = entry
        if(sha1 in mdbDataMap.keys()):
            if(sha1 not in filesWIthSameHashInMdb.keys()):
                # add file that we added first
                filesWIthSameHashInMdb[sha1] = [mdbDataMap[sha1][0]]  # first file with that hash
            filesWIthSameHashInMdb[sha1].append(fileId)
        else:
            mdbDataMap[sha1] = (fileId, fileName, fileSize, contentUnitId, contentTypeName)
        if(not contentUnitId):
            filesWithoutContentUnit.append((fileId, sha1, fileName))

    print("Create mapping (sourceId<->content_units) by sourceId for {} entries".format(len(mdbSourceContentUnitRelationData)))
    mdbSourceIdToContentUnitNameMap = {}
    # (sourceId, content_unit_id)
    for entry in mdbSourceContentUnitRelationData:
        (sourceId, contentUnitId) = entry
        if(not mdbSourceIdToContentUnitNameMap.get(sourceId, None)):
            mdbSourceIdToContentUnitNameMap[sourceId] = []
        mdbSourceIdToContentUnitNameMap[sourceId].append(contentUnitId)

    fsFolderToFilesMap = {}

    batchSize = 10000
    batchWriteToFile = 1000

    processingFilesCount = 0
    currentDate = str(datetime.datetime.now()).replace(' ', '_')

    def writeIterableToFileIfBatch(iterable, output):
        if (len(iterable) >= batchWriteToFile):
            writeIterableToFile(iterable, output)

    def writeIterableToFile(iterable, output):
        for item in iterable:
            output.write(str(item) + '\n')

    print("Start to compare mdb and index")
    with open(pathToIndexFile, "r") as indexFile, \
            open(os.path.join(analysisSummaryFolder, "validFolders_{}.txt".format(currentDate)), "w") as validFoldersFile, \
            open(os.path.join(analysisSummaryFolder, "invalidFolders_{}.txt".format(currentDate)), "w") as invalidFoldersFile, \
            open(os.path.join(analysisSummaryFolder, "filesInFsNotInMdb_{}.txt".format(currentDate)), "w") as filesInFsNotInMdbFile, \
            open(os.path.join(analysisSummaryFolder, "filesInMdbNotInFs_{}.txt".format(currentDate)), "w") as filesInMdbNotInFsFile, \
            open(os.path.join(analysisSummaryFolder, "filesWIthSameHashInMdb_{}.txt".format(currentDate)), "w") as filesWIthSameHashInMdbFile, \
            open(os.path.join(analysisSummaryFolder, "filesWIthSameHashInFs_{}.txt".format(currentDate)), "w") as filesWIthSameHashInFsFile, \
            open(os.path.join(analysisSummaryFolder, "filesWithoutContentUnit_{}.txt".format(currentDate)), "w") as filesWithoutContentUnitFile:

        for line in indexFile:
            numFilesInIndex += 1
            # some file names paths contains commas so we can not just split by comma
            lineSplitted = line.rstrip('\n').split(",")
            (hash, size, date) = lineSplitted[-3:]
            filePath = ''.join(lineSplitted[:-3])
            folderPath = os.path.dirname(filePath)
            # processing by folders
            if (not folderPath in fsFolderToFilesMap.keys()):
                if(processingFilesCount >= batchSize):
                    # processing
                    # return (validFolders, invalidFolders, filesInFsNotInMdb, filesInFsNotInMdb, filesWithNoneContentUnit)
                    (validFoldersPart, invalidFoldersPart, filesInFsNotInMdbPart, filesWIthSameHashInFsPart) = compareIndexPartAndMdb(mdbDataMap, fsFolderToFilesMap)

                    validFolders.extend(validFoldersPart)
                    invalidFolders.extend(invalidFoldersPart)
                    filesInFsNotInMdb.extend(filesInFsNotInMdbPart)
                    filesWIthSameHashInFs.update(filesWIthSameHashInFsPart)
                    fsFolderToFilesMap.clear()

                fsFolderToFilesMap[folderPath] = []
            # (hash, file_path, size, date)
            fsFolderToFilesMap[folderPath].append((hash, filePath, size, date))
            processingFilesCount+=1
        if(len(fsFolderToFilesMap.keys()) > 0):
            (validFoldersPart, invalidFoldersPart, filesInFsNotInMdbPart, filesWIthSameHashInFsPart) = compareIndexPartAndMdb(mdbSourceIdToContentUnitNameMap, fsFolderToFilesMap)
            validFolders.extend(validFoldersPart)
            invalidFolders.extend(invalidFoldersPart)
            filesInFsNotInMdb.extend(filesInFsNotInMdbPart)
            filesWIthSameHashInFs.update(filesWIthSameHashInFsPart)
            fsFolderToFilesMap.clear()

        filesInMdbNotInFs.extend(mdbSourceIdToContentUnitNameMap.items())

        writeIterableToFile([("hash", "path", "size", "date")] + filesInFsNotInMdb, filesInFsNotInMdbFile)
        writeIterableToFile([("hash", "fileId", "fileName", "fileSize", "contentUnitId", "contentTypeId")] + filesInMdbNotInFs, filesInMdbNotInFsFile)
        writeIterableToFile([("folderPath", "content_units")] + validFolders, validFoldersFile)
        writeIterableToFile([("folderPath", "errors")] + invalidFolders, invalidFoldersFile)
        writeIterableToFile([("hash", "fileId")] + list(filesWIthSameHashInMdb.items()), filesWIthSameHashInMdbFile)
        writeIterableToFile([("folder", "(hash" , "firstFounded", "current)")] + list(filesWIthSameHashInFs.items()), filesWIthSameHashInFsFile)
        writeIterableToFile([("fileId", "hash", "fileName")] + filesWithoutContentUnit, filesWithoutContentUnitFile)
        printToFileAndConsole(os.path.join(analysisSummaryFolder, "summary_{}.txt".format(currentDate)),
                                  [
                                      "\n********\nSummary\n********",
                                      "Files in fs: {}".format(numFilesInIndex),
                                      "File entries in mdb: {}".format(len(mdbFilesData)),
                                      "Files in fs not in mdb: {}".format(len(filesInFsNotInMdb)),
                                      "Files in mdb not in fs: {}".format(len(filesInMdbNotInFs)),
                                      "Num of \"duplicated\" (in one folder, several files with same hash) files in mdb: {}".format(len(filesWIthSameHashInMdb.items())),
                                      "Num of \"duplicated\" files (by hash) in fs (compared only inside folders): {}".format(len(filesWIthSameHashInFs.items())),
                                      "Num of 'valid' folders (one 'significant' content unit (not None and not KITEI_MAKOV)): {}".format(len(validFolders)),
                                      "Num of invalid folders: {}".format(len(invalidFolders))
                                  ]
                              )


if __name__ == '__main__':
    main(sys.argv[1:])

