#!/usr/bin/python3

import sys
import os


import json
import psycopg2
import pickle
import datetime
import json

# pip install pyexcel
import pyexcel as p

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


    indexFilePath = normalizeFilePath("/mnt/DATA/Documents/BB_MDB/Mapping/20171013-archive_sorted.csv")
    mappingFilePath = normalizeFilePath("/mnt/DATA/Documents/BB_MDB/Mapping/Mapping_06.10.2017.xlsx")
    postgresqlOptFilePath = normalizeFilePath("./postgresqlOpt.json")
    destFolderPath = normalizeFilePath('./')
    fetchDataFromDbAndSaveToFile = False
    mdbDFilesFileName = "mdbFilesFromMdb"
    mdbSOurceOCntentUnitRelationFileName = "mdbContentUnitSourceRelation"

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
        print("Query files data...")
        cur.execute("select encode(f.sha1, 'hex') as sha1, f.id as file_id, f.name as file_name, f.size as file_size, cu.id as content_unit_id, ct.name as content_unit_type_name   from files f left join content_units cu on f.content_unit_id = cu.id left join content_types ct on cu.type_id=ct.id where f.sha1 is not NULL")
        #(sha1, fileId, fileName, fileSize, content_unit_id, content_unit_type_name)
        mdbFilesData = cur.fetchall()

        # for sourceId <-> content_unit mapping
        print("Query sourceId<->contentUnitType data...")
        cur.execute("select cu.id as content_unit_id, s.id as sourceId from content_units_sources cus join sources s on cus.source_id = s.id join content_units cu on cus.content_unit_id = cu.id")
        #(sourceId, content_unit_id)
        mdbContentUnitSourceRelationData = cur.fetchall()

        cur.close()
        connection.close()
        print("Save database data as:\n\t'{}'\nand\n\t'{}'".format(mdbDFilesFilePath, mdbSOurceOCntentUnitRelationFilePath))
        with open(mdbDFilesFilePath, 'wb') as mdbFilesFile, open(mdbSOurceOCntentUnitRelationFilePath, 'wb') as mdbSOurceOCntentUnitRelationFile:
            pickle.dump(mdbFilesData, mdbFilesFile)
            pickle.dump(mdbContentUnitSourceRelationData, mdbSOurceOCntentUnitRelationFile)
    else:
        print("Load database data from:\n\t{}\nand\n\t{}".format(mdbDFilesFilePath, mdbSOurceOCntentUnitRelationFilePath))
        with open(mdbDFilesFilePath, 'rb') as mdbFilesFile, open(mdbSOurceOCntentUnitRelationFilePath, 'rb') as mdbSOurceOCntentUnitRelationFile:
            mdbFilesData = pickle.load(mdbFilesFile)
            mdbContentUnitSourceRelationData = pickle.load(mdbSOurceOCntentUnitRelationFile)

    # get data from mapping file, skip headers
    sheet = p.get_sheet(file_name=mappingFilePath, name_columns_by_row=0)

    numFilesInIndex = 0

    filesInMdbNotInFs = []
    filesInFsNotInMdb = []
    filesInFsInMdbWIthoutCOntentUnit = []

    filesWIthSameHashInMdb = {}
    #filesAtFsWIthSameHashInFolderMap = {}
    filesInOneFolderWIthSameHash = []
    foldersWIthoutSignificantContentUnits = []
    foldersWIthMultipleSIgnificantContentUnits = []
    foldersWIthoutBeavodaPartInPath = []
    foldersWIthBeavodaPartButNotInMappingFile = []
    foldersWithWrongContentUnitSOurceMapping = []

    filesEntriesWithoutContentUnitInMdb = []

    # every file in folder has entry in Mdb (files table) + every file belong one (or two (with KITEI_MEKOV)) content unit
    validFolders = []
    # with description
    invalidFolders = []

    print("Create mapping by hash for {} mdb entries".format(len(mdbFilesData)))
    mdbDataMap = {}
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
            filesEntriesWithoutContentUnitInMdb.append((fileId, sha1, fileName))

    print("Create mapping (contentUnit -> sourcesIds) for {} entries".format(len(mdbContentUnitSourceRelationData)))
    mdbContentUnitIdToSourceIdMap = {}
    # (content_unit_id, sourceId)
    for entry in mdbContentUnitSourceRelationData:
        (contentUnitId, sourceId) = entry
        if(not mdbContentUnitIdToSourceIdMap.get(contentUnitId, None)):
            mdbContentUnitIdToSourceIdMap[contentUnitId] = []
        mdbContentUnitIdToSourceIdMap[contentUnitId].append(sourceId)

    print("Create mapping (folderPath->source_id)")
    folderToSourceIdMap = {}
    # (content_unit_id, sourceId)

    rowCounter = 0
    for row in sheet:
        if (row[0] == ""):
            continue
        mbId = str(int(row[0]))
        name = row[1]
        path = row[2]
        if(path == ""):
            continue
        path = path.replace("\\","/")
        folderToSourceIdMap[path] = mbId
        rowCounter += 1
    print("\tMapped {} entries".format(rowCounter))

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
    with open(indexFilePath, "r") as indexFile, \
            open(os.path.join(analysisSummaryFolder, "summary_{}.txt".format(currentDate)), "w") as summaryFile, \
            open(os.path.join(analysisSummaryFolder, "validFolders_{}.txt".format(currentDate)), "w") as validFoldersFile, \
            open(os.path.join(analysisSummaryFolder, "invalidFolders_{}.txt".format(currentDate)), "w") as invalidFoldersFile, \
            open(os.path.join(analysisSummaryFolder, "filesInFsNotInMdb_{}.txt".format(currentDate)), "w") as filesInFsNotInMdbFile, \
            open(os.path.join(analysisSummaryFolder, "filesInMdbNotInFs_{}.txt".format(currentDate)), "w") as filesInMdbNotInFsFile, \
            open(os.path.join(analysisSummaryFolder, "filesInFsInMdbWIthoutCOntentUnit_{}.txt".format(currentDate)), "w") as filesInFsInMdbWIthoutCOntentUnitFile, \
            open(os.path.join(analysisSummaryFolder, "filesWIthSameHashInMdb_{}.txt".format(currentDate)), "w") as filesWIthSameHashInMdbFile, \
            open(os.path.join(analysisSummaryFolder, "filesWIthSameHashInFs_{}.txt".format(currentDate)), "w") as filesWIthSameHashInFsFile, \
            open(os.path.join(analysisSummaryFolder, "filesWithoutContentUnit_{}.txt".format(currentDate)), "w") as filesWithoutContentUnitFile, \
            open(os.path.join(analysisSummaryFolder, "foldersWIthoutSignificantContentUnits_{}.txt".format(currentDate)), "w") as foldersWIthoutSignificantContentUnitsFile, \
            open(os.path.join(analysisSummaryFolder, "foldersWIthMultipleSIgnificantContentUnits_{}.txt".format(currentDate)), "w") as foldersWIthMultipleSIgnificantContentUnitsFile, \
            open(os.path.join(analysisSummaryFolder, "foldersWIthoutBeavodaPartInPathFile_{}.txt".format(currentDate)), "w") as foldersWIthoutBeavodaPartInPathFile, \
            open(os.path.join(analysisSummaryFolder, "foldersWIthBeavodaPartButNotInMapping_{}.txt".format(currentDate)), "w") as foldersWIthBeavodaPartButNotInMappingFileFile, \
            open(os.path.join(analysisSummaryFolder, "foldersWithWrongContentUnitSOurceMapping_{}.txt".format(currentDate)), "w") as foldersWithWrongContentUnitSOurceMappingFile:


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
                    print("Processing {} folders".format(len(fsFolderToFilesMap.keys())))
                    # processing
                    # return (validFolders, invalidFolders, filesInFsNotInMdb, filesInFsNotInMdb, filesWithNoneContentUnit)
                    (validFoldersPart, invalidFoldersPart, filesInFsNotInMdbPart, filesInFsInMdbWIthoutCOntentUnitPart,
                     filesInOneFolderWIthSameHashPart, foldersWIthoutSignificantContentUnitsPart, foldersWIthMultipleSIgnificantContentUnitsPart,
                     foldersWIthoutBeavodaPartInPathPart, foldersWIthBeavodaPartButNotInMappingFilePart, foldersWithWrongContentUnitSOurceMappingPart) = compareIndexPartAndMdb(mdbDataMap, mdbContentUnitIdToSourceIdMap, fsFolderToFilesMap, folderToSourceIdMap)

                    validFolders.extend(validFoldersPart)
                    invalidFolders.extend(invalidFoldersPart)
                    filesInFsNotInMdb.extend(filesInFsNotInMdbPart)
                    filesInFsInMdbWIthoutCOntentUnit.extend(filesInFsInMdbWIthoutCOntentUnitPart)
                    # filesAtFsWIthSameHashInFolderMap.update(filesAtFsWIthSameHashInFolderMapPart)
                    filesInOneFolderWIthSameHash.extend(filesInOneFolderWIthSameHashPart)
                    foldersWIthoutSignificantContentUnits.extend(foldersWIthoutSignificantContentUnitsPart)
                    foldersWIthMultipleSIgnificantContentUnits.extend(foldersWIthMultipleSIgnificantContentUnitsPart)
                    foldersWIthoutBeavodaPartInPath.extend(foldersWIthoutBeavodaPartInPathPart)
                    foldersWIthBeavodaPartButNotInMappingFile.extend(foldersWIthBeavodaPartButNotInMappingFilePart)
                    foldersWithWrongContentUnitSOurceMapping.extend(foldersWithWrongContentUnitSOurceMappingPart)

                    fsFolderToFilesMap.clear()
                    processingFilesCount = 0

                fsFolderToFilesMap[folderPath] = []
            # (hash, file_path, size, date)
            fsFolderToFilesMap[folderPath].append((hash, filePath, size, date))
            processingFilesCount+=1
        if(len(fsFolderToFilesMap.keys()) > 0):
            print("Processing {} folders".format(len(fsFolderToFilesMap.keys())))
            (validFoldersPart, invalidFoldersPart, filesInFsNotInMdbPart, filesInFsInMdbWIthoutCOntentUnitPart,
             filesInOneFolderWIthSameHashPart, foldersWIthoutSignificantContentUnitsPart, foldersWIthMultipleSIgnificantContentUnitsPart,
             foldersWIthoutBeavodaPartInPathPart, foldersWIthBeavodaPartButNotInMappingFilePart, foldersWithWrongContentUnitSOurceMappingPart) = compareIndexPartAndMdb(mdbDataMap, mdbContentUnitIdToSourceIdMap, fsFolderToFilesMap, folderToSourceIdMap)

            validFolders.extend(validFoldersPart)
            invalidFolders.extend(invalidFoldersPart)
            filesInFsNotInMdb.extend(filesInFsNotInMdbPart)
            filesInFsInMdbWIthoutCOntentUnit.extend(filesInFsInMdbWIthoutCOntentUnitPart)
            # filesAtFsWIthSameHashInFolderMap.update(filesAtFsWIthSameHashInFolderMapPart)
            filesInOneFolderWIthSameHash.extend(filesInOneFolderWIthSameHashPart)
            foldersWIthoutSignificantContentUnits.extend(foldersWIthoutSignificantContentUnitsPart)
            foldersWIthMultipleSIgnificantContentUnits.extend(foldersWIthMultipleSIgnificantContentUnitsPart)
            foldersWIthoutBeavodaPartInPath.extend(foldersWIthoutBeavodaPartInPathPart)
            foldersWIthBeavodaPartButNotInMappingFile.extend(foldersWIthBeavodaPartButNotInMappingFilePart)
            foldersWithWrongContentUnitSOurceMapping.extend(foldersWithWrongContentUnitSOurceMappingPart)

            fsFolderToFilesMap.clear()

        filesInMdbNotInFs.extend(mdbDataMap.items())

        writeIterableToFile([("folderPath", "content_units")] + validFolders, validFoldersFile)
        writeIterableToFile([("folderPath", "errors")] + invalidFolders, invalidFoldersFile)
        writeIterableToFile([("hash", "path", "size", "date")] + filesInFsNotInMdb, filesInFsNotInMdbFile)
        writeIterableToFile([("hash", "fileId", "fileName", "fileSize", "contentUnitId", "contentTypeId")] + filesInMdbNotInFs, filesInMdbNotInFsFile)
        writeIterableToFile([("hash", "filePath", "length", "date")] + filesInFsInMdbWIthoutCOntentUnit, filesInFsInMdbWIthoutCOntentUnitFile)

        writeIterableToFile([("hash", "fileId")] + list(filesWIthSameHashInMdb.items()), filesWIthSameHashInMdbFile)
        # writeIterableToFile([("folder", "(hash" , "firstFounded", "current)")] + list(filesAtFsWIthSameHashInFolderMap.items()), filesWIthSameHashInFsFile)
        writeIterableToFile([("hash", "folderPath", "firstFileName", "current")] + filesInOneFolderWIthSameHash, filesWIthSameHashInFsFile)
        writeIterableToFile([("fileId", "hash", "fileName")] + filesEntriesWithoutContentUnitInMdb, filesWithoutContentUnitFile)


        writeIterableToFile([("folderPath", "contentUnits")] + foldersWIthoutSignificantContentUnits, foldersWIthoutSignificantContentUnitsFile)
        writeIterableToFile([("folderPath", "num of significant units", "significant units")] + foldersWIthMultipleSIgnificantContentUnits, foldersWIthMultipleSIgnificantContentUnitsFile)
        writeIterableToFile([("folderPath", "contentUnits")] + foldersWIthoutBeavodaPartInPath, foldersWIthoutBeavodaPartInPathFile)
        writeIterableToFile([("folderPath", "contentUnits")] + foldersWIthBeavodaPartButNotInMappingFile, foldersWIthBeavodaPartButNotInMappingFileFile)
        writeIterableToFile([("folderPath", "significantCOntentUnitId", "sourceIdFromMappingFile", "sourceIdsBySignificantContentUnitId")] + foldersWithWrongContentUnitSOurceMapping, foldersWithWrongContentUnitSOurceMappingFile)
        printToFileAndConsole(summaryFile,
                                  [
                                      "\n********\nSummary\n********",
                                      "Files in fs: {}".format(numFilesInIndex),
                                      "File entries in mdb: {}".format(len(mdbFilesData)),
                                      "Files in fs not in mdb: {}".format(len(filesInFsNotInMdb)),
                                      "Files in mdb not in fs: {}".format(len(filesInMdbNotInFs)),
                                      "Num of \"duplicated\" (in one folder, several files with same hash) files in mdb: {}".format(len(filesWIthSameHashInMdb.items())),
#                                      "Num of \"duplicated\" files (by hash) in fs (compared only inside folders): {}".format(len(filesAtFsWIthSameHashInFolderMap.items())),
                                      "Num of 'valid' folders : {}\n\t*(one 'significant' content unit (not None and not KITEI_MAKOV)\n\t*in ____beavoda folder\n\t*entry in mapping file with not empty path\n\t*sourceId from mapping file IN folder's sourceIds (got by content_unit_id -> sourceIds mapping))".format(len(validFolders)),
                                      "Num of invalid folders: {}".format(len(invalidFolders))
                                  ]
                              )


if __name__ == '__main__':
    main(sys.argv[1:])

