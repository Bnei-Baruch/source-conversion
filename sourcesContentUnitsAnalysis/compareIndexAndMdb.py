#!/usr/bin/python3

import os.path

def compareIndexPartAndMdb(mdbDataMap, mdbContentUnitIdToSourceIdMap, fsFolderToFilesMap, folderToSourceIdMappingFileMap):
    validFolders = []
    invalidFolders = []

    filesInFsNotInMdb = []
    filesInFsInMdbWIthoutCOntentUnit = []
    # filesAtFsWIthSameHashInFolderMap = {}
    filesInOneFolderWIthSameHash = []
    foldersWIthoutSignificantContentUnits = []
    foldersWIthMultipleSIgnificantContentUnits = []
    foldersWIthoutBeavodaPartInPath = []
    foldersWIthBeavodaPartButNotInMappingFile = []
    foldersWithWrongContentUnitSOurceMapping = []

    folderMappedFilesHashes = {}
    for fsFolderPath in fsFolderToFilesMap.keys():
        fsFileTuplesInFolder = fsFolderToFilesMap[fsFolderPath]
        folderMappedFilesHashes.clear()
        folderErrors = []
        fsFolderContentUnitsMap = {}
        for fsFileTuple in fsFileTuplesInFolder:
            # (sha1, filePath, size, date)
            (sha1, filePath, size, date) = fsFileTuple

            fileEntryInMdb = mdbDataMap.get(sha1, None)

            if (fileEntryInMdb):
                # (fileId, fileName, fileSize, content_unit_id, content_unit_type_name)
                (fileId, fileName, fileSize, contentUnitId, contentUnitTypeName) = fileEntryInMdb
                if(not fsFolderContentUnitsMap.get(contentUnitId, None)):
                    fsFolderContentUnitsMap[contentUnitId] = (contentUnitId, contentUnitTypeName, 1)
                    if(contentUnitId == None):
                        filesInFsInMdbWIthoutCOntentUnit.append(fsFileTuple)
                else:
                    (id, typename, counter) = fsFolderContentUnitsMap[contentUnitId]
                    fsFolderContentUnitsMap[contentUnitId] = (id, typename, counter + 1)
                folderMappedFilesHashes[sha1] = os.path.basename(filePath)
                del mdbDataMap[sha1]
            else:
                if (folderMappedFilesHashes.get(sha1, None)):
                    # already mapped in folder (may be same file with other name)
                    # validFolder = False
                    # (hash, folderPath, firstFileName, current)
                    filesInOneFolderWIthSameHash.append((sha1, fsFolderPath, folderMappedFilesHashes[sha1], os.path.basename(filePath)))

                    # if(not filesAtFsWIthSameHashInFolderMap.get(sha1, None)):
                    #     filesAtFsWIthSameHashInFolderMap[fsFolderPath] = []
                    # filesAtFsWIthSameHashInFolderMap[fsFolderPath].append((sha1, folderMappedFilesHashes[sha1], os.path.basename(filePath)))

                else:
                    # (sha1, filePath, size, date)
                    filesInFsNotInMdb.append(fsFileTuple)
                    # validFolder = False

        fsFolderContentUnits = set(fsFolderContentUnitsMap.values())

        isValidFolder = True

        #check folder's content_units
        significantContentUnits = []
        for fsFolderContentUnit in fsFolderContentUnits:
            if(fsFolderContentUnit[1] == 'KITEI_MAKOR' or fsFolderContentUnit[0] == None):
                continue
            significantContentUnits.append(fsFolderContentUnit)
        if(len(significantContentUnits) == 0):
            isValidFolder = False
            folderErrors.append("No content units for folder: {}".format(fsFolderPath))
            # (folderPath, content_units)
            foldersWIthoutSignificantContentUnits.append((fsFolderPath, fsFolderContentUnits))
        elif(len(significantContentUnits) > 1):
            isValidFolder = False
            folderErrors.append("Multiple content units for folder: {}, {}".format(fsFolderPath, fsFolderContentUnits))
            # (folderPath, content_units)
            foldersWIthMultipleSIgnificantContentUnits.append((fsFolderPath, len(significantContentUnits), fsFolderContentUnits))
        else:
            beavodaFilePathPartIndex = fsFolderPath.find("____beavoda/")
            if(beavodaFilePathPartIndex > 0):
                asSourceIdFolder = fsFolderPath[beavodaFilePathPartIndex:]
                asLessonFolder = os.path.dirname(asSourceIdFolder)

                # we have structure:
                #   sourceIdRootFolder ->
                #        lessonFolder1
                #        lessonFolder2
                #        ...
                # and we first check 'fsFolderPath' as sourceIdRootFolder than as lessonFolder

                folderToSourceId = folderToSourceIdMappingFileMap.get(asSourceIdFolder, None)
                if(not folderToSourceId):
                    folderToSourceId = folderToSourceIdMappingFileMap.get(asLessonFolder, None)

                if(folderToSourceId):
                    folderToSourceId = int(folderToSourceId)
                    (significantCOntentUnitId, significantCOntentUnitTypeName, numOfFilesBelongsTo) = significantContentUnits[0]
                    sourcesIdsByContentUnitIdList = mdbContentUnitIdToSourceIdMap.get(significantCOntentUnitId, None)
                    if(not (sourcesIdsByContentUnitIdList and folderToSourceId in sourcesIdsByContentUnitIdList)):
                        isValidFolder = False
                        folderErrors.append("Wrong contentUnitId <-> sourceId mapping".format(fsFolderPath))
                        foldersWithWrongContentUnitSOurceMapping.append((fsFolderPath, significantCOntentUnitId, folderToSourceId, sourcesIdsByContentUnitIdList))
                else:
                    isValidFolder = False
                    folderErrors.append("No mapping at mapping file for folder {}".format(fsFolderPath))
                    foldersWIthBeavodaPartButNotInMappingFile.append((fsFolderPath, fsFolderContentUnits))
            else:
                isValidFolder = False
                folderErrors.append("No ____beavoda part in folder {}".format(fsFolderPath))
                foldersWIthoutBeavodaPartInPath.append((fsFolderPath, fsFolderContentUnits))

        if(isValidFolder):
            validFolders.append((fsFolderPath, fsFolderContentUnits))
        else:
            invalidFolders.append((fsFolderPath, folderErrors))


    return (validFolders, invalidFolders, filesInFsNotInMdb,filesInFsInMdbWIthoutCOntentUnit,
            filesInOneFolderWIthSameHash, foldersWIthoutSignificantContentUnits, foldersWIthMultipleSIgnificantContentUnits,
            foldersWIthoutBeavodaPartInPath, foldersWIthBeavodaPartButNotInMappingFile, foldersWithWrongContentUnitSOurceMapping)
