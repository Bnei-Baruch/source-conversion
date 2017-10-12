#!/usr/bin/python3

import os.path

def compareIndexPartAndMdb(mdbDataMap, fsFolderToFilesMap):
    filesInFsNotInMdb = []
    filesWIthSameHashInFsInFolderMap = {}
    validFolders = []
    invalidFolders = []

    fsFolderContentUnits = set()
    folderMappedFilesHashes = {}

    for fsFolderPath in fsFolderToFilesMap.keys():
        fsFileTuplesInFolder = fsFolderToFilesMap[fsFolderPath]
        folderMappedFilesHashes.clear()
        folderErrors = []
        fsFolderContentUnits = set()
        for fsFileTuple in fsFileTuplesInFolder:
            # (sha1, filePath, size, date)
            (sha1, filePath, size, date) = fsFileTuple

            fileEntryInMdb = mdbDataMap.get(sha1, None)

            if (fileEntryInMdb):
                # (fileId, fileName, fileSize, content_unit_id, content_unit_type_name)
                (fileId, fileName, fileSize, contentUnitId, contentTypeName) = fileEntryInMdb
                fsFolderContentUnits.add((contentUnitId, contentTypeName))
                folderMappedFilesHashes[sha1] = os.path.basename(filePath)
                del mdbDataMap[sha1]
            else:
                if (folderMappedFilesHashes.get(sha1, None)):
                    #already mapped in folder (may be same file with other name)
#                    validFolder = False
                    folderErrors.append(("Multiple files with hash: {}\nat folder {}.\nWas first: {}\n current: {}".format(sha1, fsFolderPath, folderMappedFilesHashes[sha1], os.path.basename(filePath))))

                    if(not filesWIthSameHashInFsInFolderMap.get(sha1, None)):
                        filesWIthSameHashInFsInFolderMap[fsFolderPath] = []
                    filesWIthSameHashInFsInFolderMap[fsFolderPath].append((sha1, folderMappedFilesHashes[sha1], os.path.basename(filePath)))

                else:
                    print("\tNo entry in MDB for {}".format(str(fsFileTuple)))
                    filesInFsNotInMdb.append(fsFileTuple)
#                    validFolder = False
                    folderErrors.append("No entry in MDB for {}".format(str(fsFileTuple)))

        isValidFolder = True
        #check folder's content_units
        if(len(fsFolderContentUnits) == 0):
            isValidFolder = False
            folderErrors.append("Folder invalid because no content units")
        else:
            numOfContentUnits = 0
            for fsFolderContentUnit in fsFolderContentUnits:
                if(fsFolderContentUnit[1] == 'KITEI_MAKOR' or fsFolderContentUnit[0] == None):
                    continue
                numOfContentUnits += 1
            if(numOfContentUnits == 0):
                isValidFolder = False
                folderErrors.append("Folder contains 1 'not significant' content unit (KITEI_MAKOV content unit or None): {}".format(fsFolderContentUnits))
            elif(numOfContentUnits > 1):
                isValidFolder = False
                folderErrors.append("Folder contains {} 'significant' content units: {}".format(numOfContentUnits, fsFolderContentUnits))

        if(isValidFolder):
            validFolders.append((fsFolderPath, fsFolderContentUnits))
        else:
            invalidFolders.append((fsFolderPath, folderErrors))


    return (validFolders, invalidFolders, filesInFsNotInMdb, filesWIthSameHashInFsInFolderMap)

