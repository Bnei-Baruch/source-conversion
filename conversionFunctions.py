#!/usr/bin/python3

import os
import subprocess
import shutil

# pip install pypandoc
import pypandoc

#pip install pytidylib
import tidylib

# inputPaths - folders woth doc files
# outputPath - docx results folder
# filenameToFolderMap - for moving docx to original doc folder
# sofficeErrors
def convertFromDocToDocx(inputPaths, outputPath, filenameToFolderMap, sofficeErrors):
    print("\t\tStart doc->docx convertion. WorkingPaths:{}".format(inputPaths))

    cmd = 'soffice' + ' --headless' + ' --convert-to' + ' docx' + ' --outdir ' + outputPath + ' ' + " ".join([t + '/*.doc' for t in inputPaths])
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = p.communicate()
    print("\t\t{}".format(stdout))
    if(len(stderr)>0):
        sofficeErrors.put((cmd, str(stdout), str(stderr)))
    print("\t\t\tDone converting from doc to docx. outputPath path: {}\n\t\tMove docx to correspond folders".format(outputPath))

    for docxFileName in os.listdir(outputPath):
        dirToMove = filenameToFolderMap.get(os.path.splitext(docxFileName)[0])
        if(dirToMove):
            resultFileName = docxFileName

            if(docxFileName.startswith("TMP_NAME_")):
                # 'TMP_NAME_<DIRNAME>_FILENAME' pattern
                resultFileName = ''.join(docxFileName.split("_")[3:])
            docxFilePath = outputPath + os.path.sep + docxFileName
            movedToFilePath = dirToMove + os.path.sep + resultFileName
            shutil.move(docxFilePath, movedToFilePath)
            print("\t\t\t'{}' moved to '{}'".format(docxFilePath, movedToFilePath))
        else:
            sofficeErrors.put((cmd, "Not found source dir for file: '{}'".format(docxFileName)))
    return

def convertFromDocxToHtml(sourcepath, destpath, tidyOptions, tidyErrors):
    print("\t\tStart docx->html convertion (pandoc) sourcepath:{}, destpath:{}".format(sourcepath, destpath))
    pypandoc.convert_file(sourcepath, to='html5', extra_args=['-s'], outputfile=destpath)
    print("\t\t\tDone converting from docx to html (pandoc) sourcepath:{}, destpath:{}".format(sourcepath, destpath))

    print("\t\tStart to tidy html. Input file '{}'".format(destpath))
    with open(destpath, 'r') as myfile:
        htmlFileContent = myfile.read().replace('\n', '')
    markup, errors = tidylib.tidy_document(htmlFileContent, tidyOptions)
    if(len(errors)>0):
        tidyErrors.put((destpath, str(errors)))
    with open(destpath, 'w') as myfile:
        myfile.write(markup)
    print("\t\t\tDone to tidy html file '{}'. Errors: {}".format(destpath, errors))
    return



