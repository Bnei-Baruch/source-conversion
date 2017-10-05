#!/usr/bin/python3

import os
import sys
import shutil
import fnmatch
import argparse

# fileUtils.py
import fileUtils

def main(args):
    parser = argparse.ArgumentParser(description='Copy files with preserving a structure of directories')
    parser.add_argument('srcfolder', action="store", help="Source folder")
    parser.add_argument('destfolder', action="store", help="Dest folder (with ____beavoda part)")
    parser.add_argument('-p', action="append", dest='patterns', default=['*.doc', '*.docx'], help="Extension patterns for source docs (default *.doc, *.docx)")
    parser.add_argument('--cleanDestDir', action="store_true", default=False, help="Remove dest src before")

    results = parser.parse_args(args)

    src = fileUtils.normalizeFilePath(results.srcfolder)
    patterns = list(set(results.patterns))
    dest = fileUtils.normalizeFilePath(results.destfolder)
    clearDestDir = results.cleanDestDir

    print("Copy stage runned with:\nsrc: {}\n patterns: {}\ndest: {}\n clearDestDir: {}".format(src,patterns,dest, clearDestDir))

    if (os.path.exists(dest) and clearDestDir):
        print("Clean folder {}".format(dest))
        shutil.rmtree(dest)
        print("Folder was cleaned".format(dest))

    for root, dirs, files in os.walk(src):
        for pattern in patterns:
            for filename in fnmatch.filter(files, pattern):
                try:
                    source = os.path.abspath(os.path.join(root, filename))
                    # replace() = Clean Roza's special folder names (contains +=% and others)
                    destFolder = os.path.dirname(source.replace(src, dest)).replace("+", "").replace("=", "").replace("%", "")
                    if (not os.path.exists(destFolder)):
                        os.makedirs(destFolder)

                    destFile = os.path.join(destFolder, filename)
                    if (not os.path.isfile(destFile)):
                        print("copy from: '%s'\n\tto: '%s'" % (source, destFile))
                        shutil.copyfile(source, destFile)
                    else:
                        print("File already exists. Skipped: '%s'" % (destFile))
                except OSError as err:
                    print(err)


if __name__ == '__main__':
    main(sys.argv[1:])

