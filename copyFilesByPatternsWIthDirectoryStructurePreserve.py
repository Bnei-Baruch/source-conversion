#!/usr/bin/python3

import os
import sys
import shutil
import fnmatch
import argparse

# fileUtils.py
import fileUtils

def main(args):
    parser = argparse.ArgumentParser(description='Copy files with preserving a structure of directories', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('srcfolder', action="store", help="Source folder")
    parser.add_argument('destfolder', action="store", help="Dest folder (with ____beavoda part)")
    parser.add_argument('-p', action="append", dest='patterns', default=['*.doc', '*.docx'], help="Extension patterns for sourceFilePath docs")
    parser.add_argument('--cleanDestDir', action="store_true", default=False, help="Clean dest src before")

    results = parser.parse_args(args)

    src = fileUtils.normalizeFilePath(results.srcfolder)
    patterns = list(set(results.patterns))
    dest = fileUtils.normalizeFilePath(results.destfolder)
    clearDestDir = results.cleanDestDir

    print("Copy stage runned with:\nsrc: {}\n patterns: {}\ndest: {}\n clearDestDir: {}".format(src,patterns,dest, clearDestDir))

    if (clearDestDir and os.path.exists(dest)):
        print("Clean folder {}".format(dest))
        shutil.rmtree(dest)
        print("Folder was cleaned".format(dest))

    for root, dirs, files in os.walk(src):
        for pattern in patterns:
            for filename in fnmatch.filter(files, pattern):
                try:
                    sourceFilePath = os.path.abspath(os.path.join(root, filename))
                    # replace() = Clean Roza's special folder names (contains +=% and others)
                    destFolderPath = os.path.dirname(sourceFilePath.replace(src, dest)).replace("+", "").replace("=", "").replace("%", "")
                    if (not os.path.exists(destFolderPath)):
                        os.makedirs(destFolderPath)

                    destFilePath = os.path.join(destFolderPath, filename)
                    if (not os.path.isfile(destFilePath)):
                        print("copy from: '%s'\n\tto: '%s'" % (sourceFilePath, destFilePath))
                        shutil.copyfile(sourceFilePath, destFilePath)
                    else:
                        print("File already exists. Skipped: '%s'" % (destFilePath))
                except OSError as err:
                    print(err)


if __name__ == '__main__':
    main(sys.argv[1:])

