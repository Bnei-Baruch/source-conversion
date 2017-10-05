#!/usr/bin/python3

import csv
import sys
import fnmatch
import os
import argparse
import datetime
import shutil
import json

# fileUtils.py
from fileUtils import calculatesha1, printToFileAndConsole, printIterableToFile, normalizeFilePath

# pip install pyexcel
import pyexcel as p


def main(args):
    parser = argparse.ArgumentParser(description='Analysis and docs organizing')
    parser.add_argument('srcFolder', action="store", help="Full path to sources folder")
    parser.add_argument('-mappingfilepath', action="store", help="Full path to Roza's mapping.xslx file")
    parser.add_argument('-p', action="append", dest='patterns', default=['*.doc', '*.docx'], help="Extension patterns for source docs (default *.doc, *.docx)")
    parser.add_argument('-destFolderPath', action="store", default=".", help="Path where organized and amalysis folders will be")
    parser.add_argument('-langMapFilePath', action="store", default=False, help="Lang3letter -> Lang2letter json map")
    parser.add_argument('--cleanOrganizedBefore', action="store_true", default=False, help="")
    results = parser.parse_args(args)

    srcFolder = normalizeFilePath(results.srcFolder)
    mappingFilePath = normalizeFilePath(results.mappingfilepath)
    patterns = set(results.patterns)
    destFolderPath = normalizeFilePath(results.destFolderPath)
    langMapFile = normalizeFilePath(results.langMapFilePath)
    cleanOrganizedBefore = results.cleanOrganizedBefore

    with open(langMapFile) as langMap_json:
        langMap = json.load(langMap_json)

    # result of analisys folder
    analysisSummaryFolder = os.path.join(destFolderPath, 'analysisSummary')
    os.makedirs(analysisSummaryFolder, exist_ok=True)
    organizedFolder = os.path.join(destFolderPath, 'organized')
    os.makedirs(organizedFolder, exist_ok=True)

    # get data from mapping file, skip headers
    sheet = p.get_sheet(file_name=mappingFilePath, name_columns_by_row=0)

    if(cleanOrganizedBefore):
        shutil.rmtree(organizedFolder)

    patternsToDocCount = {}
    for pattern in patterns:
        patternsToDocCount[pattern] = 0

    # (from docs filenames)
    typesWithDocsCount = {}

    langsRecognizedByDocNames = {}

    langsBySourcesCount = {}

    # total valid count
    numSourcesWithAtLeastOneValidDoc = []
    # valid by filename in current version (at least has lang part) LANG_TYPE_NAME
    # elem = (mbId, lang, type, fullpath, sha1)
    validDocsFiles = []
    # valid = has at least one valid doc file
    # elem = (mbId, counts_by_lengths)
    validSources = []
    # (mbId, path, description)
    warnings = []

    # (mbId, relativePath, description)
    docsRecognizedAndSkippedByFileName = []
    # (mbId, lang, type, relativePath, description)
    originalFileNamesRecognitionProblems = []

    analyzedDocsCounter = 0
    skippedDocsCounter = 0
    copiedDocsCounter = 0
    newFormatDocsCounter = 0

    rowsCounter = 0

    # Main cycle
    for row in sheet:
        rowsCounter += 1
        print("Process {}({}) row".format(rowsCounter, sheet.number_of_rows()))
        if (row[0] == ""):
            print("Row {} skipped because of empty mbId. Row's content: {}".format(rowsCounter, row))
            continue
        mbId = str(int(row[0]))
        name = row[1]
        path = row[2]
        # print ("mbId:'%s', name:'%s':, path:'%s'" % (mbId, name, path))
        if (not path == ""):
            sourceFolder = normalizeFilePath(os.path.join(srcFolder, path))
            if (not os.path.exists(sourceFolder)):
                warnings.append((mbId, sourceFolder, "Path is not exist"))
                continue

            langsBySource = {}
            originalSourceDocsByLang = {}

            print("\tAnalyze mbId:{}, from folder: '{}'".format(mbId, sourceFolder))
            for pattern in patterns:
                print("\t\tBy pattern '{}'".format(pattern))
                fileNamesMatchedPattern = fnmatch.filter(os.listdir(sourceFolder), pattern)
                analyzedFilesInFolderCounterByPattern = 0
                for filename in fileNamesMatchedPattern:
                    analyzedFilesInFolderCounterByPattern += 1
                    analyzedDocsCounter += 1
                    analyzedFileRelativePath = os.path.join(path, filename).replace("\\", "/")
                    if ("_im_nikud." in filename):
                        print("\t\t\tdoc #{}, file {}({}) with name '{}': skipped because marked as NIKUD".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern,
                                                                                                                  len(fileNamesMatchedPattern),
                                                                                                                  filename))
                        # (mbId, relativePath, description)
                        docsRecognizedAndSkippedByFileName.append((mbId, analyzedFileRelativePath, 'NIKUD doc skipped'))
                        continue
                    elif ("_im-nikud." in filename):
                        print(
                            "\t\t\tdoc #{}, file {}({}) with name '{}': skipped because marked as SCAN".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern, len(fileNamesMatchedPattern),
                                                                                                               filename))
                        # (mbId, relativePath, description)
                        docsRecognizedAndSkippedByFileName.append((mbId, analyzedFileRelativePath, 'SCAN doc skipped'))
                        continue
                    elif ("_full." in filename):
                        print(
                            "\t\t\tdoc #{}, file {}({}) with name '{}': skipped because marked as SCAN".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern, len(fileNamesMatchedPattern),
                                                                                                               filename))
                        # (mbId, relativePath, description)
                        docsRecognizedAndSkippedByFileName.append((mbId, analyzedFileRelativePath, 'SCAN doc skipped'))
                        continue
                    elif ("_scan." in filename):
                        print(
                            "\t\t\tdoc #{}, file {}({}) with name '{}': skipped because marked as SCAN".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern, len(fileNamesMatchedPattern),
                                                                                                               filename))
                        # (mbId, relativePath, description)
                        docsRecognizedAndSkippedByFileName.append((mbId, analyzedFileRelativePath, 'SCAN doc skipped'))
                        continue
                    elif ("_edited_academia." in filename):
                        print("\t\t\tdoc #{}, file {}({}) with name '{}': skipped because marked as EDITED".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern,
                                                                                                                   len(fileNamesMatchedPattern),
                                                                                                                   filename))
                        # (mbId, relativePath, description)
                        docsRecognizedAndSkippedByFileName.append((mbId, analyzedFileRelativePath, 'EDITED doc skipped'))
                        continue
                    elif ("_helki." in filename):
                        print("\t\t\tdoc #{}, file {}({}) with name '{}': skipped because marked as HELKI".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern,
                                                                                                                  len(fileNamesMatchedPattern),
                                                                                                                  filename))
                        # (mbId, relativePath, description)
                        docsRecognizedAndSkippedByFileName.append((mbId, analyzedFileRelativePath, 'HELKI doc skipped'))
                        continue
                    elif ("_old.." in filename):
                        print("\t\t\tdoc #{}, file {}({}) with name '{}': skipped because marked as HELKI".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern,
                                                                                                                  len(fileNamesMatchedPattern),
                                                                                                                  filename))
                        # (mbId, relativePath, description)
                        docsRecognizedAndSkippedByFileName.append((mbId, analyzedFileRelativePath, 'HELKI doc skipped'))
                        continue

                    fileNameParts = filename.split('_')

                    # some new files started wih page number (____beavoda/80_rabash/mekorot/02_igrot/1424_rb-igeret-03/1424_heb_o_rb-igeret-03.docx)
                    # this files = well formated, so we prefer to get them
                    langIndex = 0
                    try:
                        int(fileNameParts[langIndex])
                        langIndex = 1
                        newFormatDocsCounter += 1
                    except ValueError:
                        langIndex = 0
                    lang = str(fileNameParts[langIndex])

                    if(not langMap.get(lang.upper())):
                        print("Unrecognized lang at filename: {}".format(filename))
                        # (mbId, lang, type, relativePath, description)
                        originalFileNamesRecognitionProblems.append((mbId, lang, '', analyzedFileRelativePath, "Unrecognized 'lang' filename part (UNRECOGNIZED)"))
                        continue

                    # map <lang> -> total docs with that <lang>
                    if (not lang in langsRecognizedByDocNames):
                        langsRecognizedByDocNames[lang] = 1
                    else:
                        langsRecognizedByDocNames[lang] += 1

                    # map <lang> -> num of docs with <lang> for one source
                    if (not lang in langsBySource):
                        langsBySource[lang] = 1
                    else:
                        langsBySource[lang] += 1

                    # get type if it exists in filename
                    if (len(fileNameParts) > 1 + langIndex):
                        type = fileNameParts[1 + langIndex]
                        if (not type in typesWithDocsCount):
                            typesWithDocsCount[type] = 1
                        else:
                            typesWithDocsCount[type] += 1
                    else:
                        print("\t\t\tDoc #{} filename '{}' without 'type' part. Relative path: {}".format(analyzedDocsCounter, filename, analyzedFileRelativePath))
                        warnings.append((mbId, analyzedFileRelativePath, "There is not 'type' part at filename (LANG_TYPE_NAME)"))

                    physicallyFilePath = srcFolder + os.path.sep + analyzedFileRelativePath
                    fileStat = os.stat(physicallyFilePath)
                    validDocsFiles.append((mbId, lang, type, analyzedFileRelativePath, calculatesha1(physicallyFilePath), fileStat.st_size, int(fileStat.st_mtime)))

                    # map pattern -> number of valid docs by this pattern (doc -> X docs, docx -> Y docs etc)
                    patternsToDocCount[pattern] += 1

                    # check is file 'original'
                    if (lang not in originalSourceDocsByLang):
                        print("\t\t\tdoc #{}, file {}({}) with name '{}': marked as ORIGINAL for '{}' lang".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern,
                                                                                                                   len(fileNamesMatchedPattern),
                                                                                                                   filename, lang))
                        originalSourceDocsByLang[lang] = (analyzedDocsCounter, physicallyFilePath, True if langIndex == 1 else False)
                    else:
                        counter, filepath, newDoc = originalSourceDocsByLang[lang]
                        # if several new format docs at folder for one lang
                        if (langIndex == 1):
                            if (newDoc):
                                print("\t\t\tdoc #{}, file {}({}) with name '{}': marked as NEW_DUPLICATED for '{}' lang. Firstly found #{} with filename: '{}'".format(analyzedDocsCounter, analyzedFilesInFolderCounterByPattern, len(fileNamesMatchedPattern), filename, lang, counter, os.path.basename(originalSourceDocsByLang[lang][1])))
                                # (mbId, lang, type, relativePath, description)
                                originalFileNamesRecognitionProblems.append((mbId, lang, type, analyzedFileRelativePath, "Multiple NEW 'original' files for lang (NEW_DUPLICATED)"))
                            else:
                                originalSourceDocsByLang[lang] = (analyzedDocsCounter, physicallyFilePath, True)
                                print("\t\t\tdoc #{}, with name '{}': marked as REPLACED_BY_NEW for '{}' lang.\n\t\t\tdoc #{}, file {}({}) with name '{}': marked as NEW_ORIGINAL".format(counter,
                                                                                                                                                                                          os.path.basename(
                                                                                                                                                                                              filepath),
                                                                                                                                                                                          lang,
                                                                                                                                                                                          analyzedDocsCounter,
                                                                                                                                                                                          analyzedFilesInFolderCounterByPattern,
                                                                                                                                                                                          len(
                                                                                                                                                                                              fileNamesMatchedPattern),
                                                                                                                                                                                          os.path.basename(
                                                                                                                                                                                              originalSourceDocsByLang[
                                                                                                                                                                                                  lang][
                                                                                                                                                                                                  1])))
                        else:
                            print("\t\t\tdoc #{}, file {}({}) with name '{}': marked as DUPLICATED for '{}' lang. Firstly found with #{} and filename: '{}'".format(analyzedDocsCounter,
                                                                                                                                                                    analyzedFilesInFolderCounterByPattern,
                                                                                                                                                                    len(fileNamesMatchedPattern),
                                                                                                                                                                    filename,
                                                                                                                                                                    lang, counter, os.path.basename(
                                    originalSourceDocsByLang[lang][1])))
                            # (mbId, lang, type, relativePath, description)
                            originalFileNamesRecognitionProblems.append((mbId, lang, type, analyzedFileRelativePath, "Multiple 'original' files for lang (DUPLICATED)"))

            if (not destFolderPath == "" and len(originalSourceDocsByLang.keys())>0):
                folderToCopy = os.path.join(organizedFolder, mbId)
                os.makedirs(folderToCopy, exist_ok=True)
                for docLang in list(originalSourceDocsByLang.keys()):
                    docCounter, sourcePhysicallyPath, newDoc = originalSourceDocsByLang[docLang]
                    print("\t\tTry to copy 'valid' doc #{}\n\t\t\tfrom:\n\t\t\t\t{}\n\t\t\tto folder\n\t\t\t\t{}".format(docCounter, sourcePhysicallyPath, folderToCopy))
                    shutil.copy(sourcePhysicallyPath, folderToCopy)
                    copiedDocsCounter += 1
                    print("\t\t\tCopy #{} success.".format(docCounter))

            if (len(langsBySource.keys()) == 0):
                if (analyzedFilesInFolderCounterByPattern == 0):
                    warnings.append((mbId, sourceFolder, "No doc files for source at path"))
                else:
                    warnings.append((mbId, sourceFolder, "No 'valid' docs for source from {} docs".format(analyzedFilesInFolderCounterByPattern)))
            else:
                for lang in langsBySource:
                    if (lang not in langsBySourcesCount):
                        langsBySourcesCount[lang] = 1
                    else:
                        langsBySourcesCount[lang] += 1
                validSources.append((mbId, langsBySource))
        else:
            print("\tRow {} skipped because of empty path field. Row's content: {}".format(rowsCounter, row))
            warnings.append((mbId, "", "Path field in mapping file is empty"))

    linesOfSummaryToPrint = []
    linesOfSummaryToPrint.append("Langs by docs filenames:\n{}\nTypes by docs filenames:\n{}".format(langsRecognizedByDocNames, typesWithDocsCount))

    linesOfSummaryToPrint.append("Pattern to doc count:\n(pattern, count):")
    for langKey in list(patternsToDocCount.keys()):
        patternToDocCount = patternsToDocCount[langKey]
        linesOfSummaryToPrint.append("({}, {})".format(langKey, patternToDocCount))

    linesOfSummaryToPrint.append("Analyzed docs counter: {}, copiedDocsCounter: {}, newFOrmaDocsCounter: {}".format(analyzedDocsCounter, copiedDocsCounter, newFormatDocsCounter))
    linesOfSummaryToPrint.append("Total 'valid' docs (current version = all)): {}".format(len(validDocsFiles)))
    linesOfSummaryToPrint.append("Total 'valid' sources (at least one 'valid' doc): {}".format(len(validSources)))

    # print("Langs by sources count\n%s" % ()) + sorting by sources count desc
    langsBySourceCountModif = []
    for langKey in list(langsBySourcesCount.keys()):
        langCount = langsBySourcesCount[langKey]
        langsBySourceCountModif.append((langKey, langCount, langCount / len(validSources)))

    langsBySourceCountModif = sorted(langsBySourceCountModif, key=lambda x: x[1], reverse=True)
    linesOfSummaryToPrint.append("Lang to sources count\n(lang, validSourcesCount, validSourcesCount/totalValidSourcesCount):")
    for i in langsBySourceCountModif:
        linesOfSummaryToPrint.append(str(i))

    currentDate = str(datetime.datetime.now()).replace(' ', '_');

    langKeysList = list(langsRecognizedByDocNames.keys())
    spreadsheet = csv.writer(open(os.path.join(analysisSummaryFolder, 'analyze_{}.csv'.format(currentDate)), 'w'), delimiter=',')
    spreadsheet.writerow(['id'] + langKeysList)
    for validSource in validSources:
        langsBySource = validSource[1]
        sourceRow = []
        for lang in langKeysList:
            langCOunt = langsBySource.get(lang, False)
            if (langCOunt):
                sourceRow.append(langCOunt)
            else:
                sourceRow.append(0)
        spreadsheet.writerow([int(validSource[0])] + sourceRow)

    printToFileAndConsole(os.path.join(analysisSummaryFolder, 'summary_{}.txt'.format(currentDate)), linesOfSummaryToPrint)
    printIterableToFile(os.path.join(analysisSummaryFolder, 'validDocsFiles_{}.txt'.format(currentDate)), [("mbId", "lang", "type", "fullpath", "sha1")] + validDocsFiles)
    printIterableToFile(os.path.join(analysisSummaryFolder, 'validSources_{}.txt'.format(currentDate)), [("mbId", "counts_by_lengths...")] + validSources)
    printIterableToFile(os.path.join(analysisSummaryFolder, 'warnings_{}.txt'.format(currentDate)), [("mbId", "path", "description")] + warnings)
    printIterableToFile(os.path.join(analysisSummaryFolder, 'fileNamesRecognitionProblems_{}.txt'.format(currentDate)), [("mbId", "lang", "type", "relativePath", "description")] + originalFileNamesRecognitionProblems)
    printIterableToFile(os.path.join(analysisSummaryFolder, 'docsSkippedByFileName_{}.txt'.format(currentDate)), [("mbId", "relativePath", "description")] + docsRecognizedAndSkippedByFileName)


if __name__ == '__main__':
    main(sys.argv[1:])
