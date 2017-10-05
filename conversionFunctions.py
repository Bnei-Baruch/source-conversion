#!/usr/bin/python3

import os
import subprocess

# pip install pypandoc
import pypandoc

#pip install pytidylib
import tidylib

# sourcePath - source file
# resultDocxFileName - (only output dir can be setted for soffice)
def convertFromDocToDocx(workingPath, sofficeErrors):
    print("\tStart doc->docx convertion. WorkingPath:{}".format(workingPath))
    cmd = 'soffice' + ' --headless' + ' --convert-to' + ' docx' + ' --outdir ' + workingPath + ' ' + workingPath+os.path.sep+"*.doc"
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = p.communicate()
    print("\t{}".format(stdout))
    if(len(stderr)>0):
        sofficeErrors.append((cmd, workingPath, stderr))
    print("\t\tDone converting from doc to docx. Working path: {}".format(workingPath))
    return

def convertFromDocxToHtml(sourcepath, destpath, tidyOptions, tidyErrors):
    print("\tStart convert from docx to html (pandoc) sourcepath:{}, destpath:{}".format(sourcepath, destpath))
    pypandoc.convert_file(sourcepath, to='html5', extra_args=['-s'], outputfile=destpath)
    print("\t\tDone converting from docx to html (pandoc) sourcepath:{}, destpath:{}".format(sourcepath, destpath))

    print("\tStart to tidy html. Input file '{}'".format(destpath))
    with open(destpath, 'r') as myfile:
        htmlFileContent = myfile.read().replace('\n', '')
    markup, errors = tidylib.tidy_document(htmlFileContent, tidyOptions)
    if(len(errors)>0):
        tidyErrors.append((destpath, errors))
    with open(destpath, 'w') as myfile:
        myfile.write(markup)
    print("\t\tDone to tidy html file '{}'. Errors: {}".format(destpath, errors))
    return



