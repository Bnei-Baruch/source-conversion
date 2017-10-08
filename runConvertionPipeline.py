#!/usr/bin/python3

import sys
import os
import argparse

import copyFilesByPatternsWIthDirectoryStructurePreserve
import organizeOriginalDocsByMBId
import convertToHtml


# fileUtils.py
from fileUtils import normalizeFilePath

def main(args):
    parser = argparse.ArgumentParser(description='Run whole pipeline (copy, convert, organize)', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    #in
    parser.add_argument('srcFolderPath', action="store", help="Sources folder (PATH/____beavoda/")
    parser.add_argument('-destFolderPath', action="store", default=".", help="Dest path (all stages folders will be here)")

    #copy stage
    parser.add_argument('--skipCopy', action="store_true", default=False, help="Skip copy stage")
    parser.add_argument('-copyPathPrefix', action="store", default="____beavoda", help="Paths at mapping xlsx file has ____beavoda/ prefix (4.10.2017)")
    parser.add_argument('-copyPatterns', action="append", default=['*.doc', '*.docx'], help="Patterns for source docs copy stage filtering")

    #organize stage
    parser.add_argument('--skipOrganize', action="store_true", default=False, help="Skip organize stage")
    parser.add_argument('-mappingFilePath', action="store", default="./mapping.xlsx", help="Path to Roza's maping file")
    parser.add_argument('-organizePatterns', action="append", default=['*.doc', '*.docx'], help="Patterns for files filter")
    parser.add_argument('-langMapFilePath', action="store", default="./langMap.json", help="Map lang3letters -> lang2letters at JSON format")

    #converting stage
    parser.add_argument('--skipConvert', action="store_true", default=False, help="Skip convert stage")
    parser.add_argument('-postgresqlOptFilePath', action="store", default="./postgresqlOpt.json", help="postgres connection params as json")
    parser.add_argument('-tidyOptionsFilePath', action="store", default="./tidyOptions.json", help="options for tidy (html -> cleaned html) as json")

    results = parser.parse_args(args)

    srcFolderPath = normalizeFilePath(results.srcFolderPath)
    destFolderPath = normalizeFilePath(results.destFolderPath)
    skipCopy = results.skipCopy
    copyPathPrefix = results.copyPathPrefix
    copyPatterns = set(results.copyPatterns)

    skipOrganize = results.skipOrganize
    mappingFilePath = normalizeFilePath(results.mappingFilePath)
    organizePatterns = results.organizePatterns
    langMapFilePath = normalizeFilePath(results.langMapFilePath)

    skipConvert = results.skipConvert
    postgresqlOptFilePath = normalizeFilePath(results.postgresqlOptFilePath)
    tidyOptionsFilePath = normalizeFilePath(results.tidyOptionsFilePath)


    #COPY
    copiedFolderPath = destFolderPath + os.path.sep + "copied/" + os.path.sep + copyPathPrefix
    if(not skipCopy):
        copyPatternsArgs = [None]*(len(copyPatterns)*2)
        copyPatternsArgs[::2] = ["-p" for t in copyPatterns]
        copyPatternsArgs[1::2] = copyPatterns
        #srcfolder destfolder [-p pattern -p pattern] --cleanDestDir
        print("Start copy stage")
        copyFilesByPatternsWIthDirectoryStructurePreserve.main([srcFolderPath, copiedFolderPath] + copyPatternsArgs)
    else:
        print("Skip copy stage")

    #ORGANIZE
    if(not skipOrganize):
        organizePatternsArgs = [None]*(len(organizePatterns)*2)
        organizePatternsArgs[::2] = ["-p" for t in organizePatterns]
        organizePatternsArgs[1::2] = organizePatterns
        # srcFolderPath mappingfilepath [-p pattern -p pattern] destFolderPath -langMapFile
        print("Start organize stage")
        organizeOriginalDocsByMBId.main([destFolderPath + os.path.sep + "copied/", "-mappingfilepath", mappingFilePath, "-destFolderPath", destFolderPath, "-langMapFilePath", langMapFilePath, "--cleanOrganizedBefore"] + organizePatternsArgs)
    else:
        print("Skip organize stage")

    if(not skipConvert):
        # srcFolderPath destFolderPath [-langMapFilePath langMapFilePath] [-postgresqlOptFilePath psqlOptPath]
        print("Start convert stage")
        convertToHtml.main([destFolderPath + os.path.sep + "organized/", "-destFolderPath", destFolderPath, "-langMapFilePath", langMapFilePath, "-postgresqlOptFilePath", postgresqlOptFilePath, "-tidyOptionsFilePath", tidyOptionsFilePath])
    else:
        print("Skip convert stage")

if __name__ == '__main__':
    main(sys.argv[1:])

