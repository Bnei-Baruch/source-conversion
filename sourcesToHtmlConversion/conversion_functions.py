#!/usr/bin/python3
import json
import os
import subprocess
import shutil
import tempfile
import zipfile

import pypandoc
import tidylib

from lxml import etree
from lxml.html.clean import Cleaner

# inputPaths - folders woth doc files
# outputPath - docx results folder
# filenameToFolderMap - for moving docx to original doc folder
# sofficeErrors
def convertFromDocToDocx(inputPaths, outputPath, filenameToFolderMap, sofficeErrors):
    print("\t\tStart doc->docx convertion. WorkingPaths:{}".format(inputPaths))

    cmd = 'soffice' + ' --headless' + ' --convert-to' + ' docx' + ' --outdir ' + outputPath + ' ' + " ".join(
        [t + '/*.doc' for t in inputPaths])
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = p.communicate()
    print("\t\t{}".format(stdout))
    if len(stderr) > 0:
        sofficeErrors.put((cmd, str(stdout), str(stderr)))
    print("\t\t\tDone converting from doc to docx. outputPath path: {}\n\t\tMove docx to correspond folders".format(
        outputPath))

    for docxFileName in os.listdir(outputPath):
        dirToMove = filenameToFolderMap.get(os.path.splitext(docxFileName)[0])
        if dirToMove:
            resultFileName = docxFileName

            if docxFileName.startswith("TMP_NAME_"):
                # 'TMP_NAME_<DIRNAME>_FILENAME' pattern
                resultFileName = ''.join(docxFileName.split("_")[3:])
            docxFilePath = outputPath + os.path.sep + docxFileName
            movedToFilePath = dirToMove + os.path.sep + resultFileName
            shutil.move(docxFilePath, movedToFilePath)
            print("\t\t\t'{}' moved to '{}'".format(docxFilePath, movedToFilePath))
        else:
            sofficeErrors.put((cmd, "Not found source dir for file: '{}'".format(docxFileName)))
    return


html_cleaner = Cleaner(kill_tags=['img'])


def convertFromDocxToHtml(src, dest, tidy_options, tidy_errors):
    # print("\t\tfix docx smartTag src:{}".format(src))
    # try:
    #     fix_smarttags_in_docx(src)
    # except Exception as ex:
    #     print("\t\t\tError fixing docx src:{}, error:".format(src, ex))

    print("\t\tStart docx->html convertion (pandoc) src:{}, dest:{}".format(src, dest))
    try:
        pypandoc.convert_file(src, to='html5', extra_args=['-s'], outputfile=dest)
    except RuntimeError as ex:
        print("\t\t\tRuntime Error converting from docx to html (pandoc) src:{}, dest:{}, error:".format(src, dest, ex))
    except OSError:
        print("\t\t\tpandoc wasn't found !!! please install first")
        raise RuntimeError("pandoc is not installed")
    # else:
    #     print(
    #         "\t\t\tDone converting from docx to html (pandoc) src:{}, dest:{}".format(src, dest))

    print("\t\tStart to tidy html. Input file '{}'".format(dest))
    with open(dest, 'r') as f:
        html_file_content = f.read().replace('\n', '')

    markup, errors = tidylib.tidy_document(html_file_content, tidy_options)
    if len(errors) > 0:
        tidy_errors.put((dest, str(errors)))
    else:
        with open(dest, 'w') as f:
            f.write(html_cleaner.clean_html(markup))
    print("\t\t\tDone to tidy html file '{}'. Errors: {}".format(dest, errors))

    return


xslt_root = etree.XML('''
    <xsl:stylesheet
            xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
            xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            version="1.0">
        <xsl:output method="xml" version="1.0" encoding="UTF-8" standalone="yes"/>
        <xsl:template match="@* | node()" name="identity">
            <xsl:copy>
                <xsl:apply-templates select="@* | node()"/>
            </xsl:copy>
        </xsl:template>
        <xsl:template match="w:smartTag">
            <w:r><w:t><xsl:value-of select="." /></w:t></w:r>
        </xsl:template>
    </xsl:stylesheet>
            ''')
transform = etree.XSLT(xslt_root)


def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for fn in files:
            absfn = os.path.join(root, fn)
            zfn = absfn[len(path) + len(os.sep):]  # XXX: relative path
            ziph.write(absfn, zfn)


def fix_smarttags_in_docx(src):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(src) as zf:
            zf.extractall(path=tmp_dir)

        doc_xml = os.path.join(tmp_dir, 'word', 'document.xml')
        doc = etree.parse(doc_xml)
        result_tree = transform(doc)
        result_tree.write_output(doc_xml)

        zipf = zipfile.ZipFile(src, 'w', zipfile.ZIP_DEFLATED)
        zipdir(tmp_dir, zipf)
        zipf.close()


if __name__ == '__main__':
    files = [
        'eng_pticha',
    ]

    base_dir = '/tmp/'
    with open('/home/edos/projects/source-conversion/conversion/tidy.json') as f:
        tidy_conf = json.load(f)

    for f in files:
        fsrc = "{}{}.docx".format(base_dir, f)
        fdest = "{}{}.html".format(base_dir, f)
        convertFromDocxToHtml(fsrc, fdest, tidy_conf, None)
