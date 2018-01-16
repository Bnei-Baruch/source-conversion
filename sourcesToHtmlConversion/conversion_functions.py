#!/usr/bin/python3
import json
import os
import subprocess
import shutil

# pip install pypandoc
import pypandoc

# pip install pytidylib
import tidylib


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


def convertFromDocxToHtml(src, dest, tidy_options, tidy_errors):
    print("\t\tStart docx->html convertion (pandoc) src:{}, dest:{}".format(src, dest))
    try:
        pypandoc.convert_file(src, to='html5', extra_args=['-s'], outputfile=dest)
    except RuntimeError as ex:
        print("\t\t\tRuntime Error converting from docx to html (pandoc) src:{}, dest:{}, error:".format(src, dest, ex))
    except OSError:
        print("\t\t\tpandoc wasn't found !!! please install first")
        raise RuntimeError("pandoc is not installed")
    else:
        print(
            "\t\t\tDone converting from docx to html (pandoc) src:{}, dest:{}".format(src, dest))

    print("\t\tStart to tidy html. Input file '{}'".format(dest))
    with open(dest, 'r') as f:
        html_file_content = f.read().replace('\n', '')

    markup, errors = tidylib.tidy_document(html_file_content, tidy_options)
    if len(errors) > 0:
        tidy_errors.put((dest, str(errors)))
    else:
        with open(dest, 'w') as f:
            f.write(markup)
    print("\t\t\tDone to tidy html file '{}'. Errors: {}".format(dest, errors))

    return


if __name__ == '__main__':
    files = [
        'ukr_text_1984-01-2-matarat-hevra-2_rabash',
        '0012_rus_t_rb-984-01-2-matarat-hevra-2',
        '0012_heb_o_rb-1984-01-2-matarat-hevra-2',
        '0012_eng_t_rb-1984-01-2-matarat-hevra-2',
        'fre_t_rb-1984-17-matarat-hevra-2'
    ]

    base_dir = '/home/edos/projects/source-conversion/conversion/converted/he3tEpLu/'
    with open('/home/edos/projects/source-conversion/conversion/tidy.json') as f:
        tidy_conf = json.load(f)

    for f in files:
        fsrc = "{}{}.docx".format(base_dir, f)
        fdest = "{}{}.html".format(base_dir, f)
        convertFromDocxToHtml(fsrc, fdest, tidy_conf, None)
