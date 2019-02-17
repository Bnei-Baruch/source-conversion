#!/usr/bin/python3

import os
import sys
import shutil
import fnmatch
import argparse
import datetime

import pyfastcopy # Can be removed once on python 3.8

# fileUtils.py
from sourcesToHtmlConversion import file_utils


def main(args):
    parser = argparse.ArgumentParser(description='Copy files with preserving a structure of directories',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('src', action="store", help="Source folder")
    parser.add_argument('dest', action="store", help="Dest folder (with ____beavoda part)")
    parser.add_argument('-work_dir', action="store", default="./conversion", help="working directory")
    parser.add_argument('-p', action="append", dest='patterns', default=['*.doc', '*.docx', '*.pdf'],
                        help="Extension patterns for src_path docs")
    parser.add_argument('--clean', action="store_true", default=False, help="Clean dest src before")

    params = parser.parse_args(args)

    src = file_utils.normalizeFilePath(params.src)
    dest = file_utils.normalizeFilePath(params.dest)
    work_dir = file_utils.normalizeFilePath(params.work_dir)
    patterns = list(set(params.patterns))

    print("""Copy stage params: 
    \tsrc={}
    \tdest={}
    \twork_dir={}
    \tpatterns={}
    \tclean={}"""
          .format(src, dest, work_dir, patterns, params.clean))

    copied_files = []
    skipped_files = []

    copied = 0
    skipped = 0

    if params.clean and os.path.exists(dest):
        print("Cleaning dest folder")
        shutil.rmtree(dest)

    for root, dirs, files in os.walk(src):
        for pattern in patterns:
            for filename in fnmatch.filter(files, pattern):
                try:
                    src_path = os.path.abspath(os.path.join(root, filename))

                    # replace() = Clean Roza's special folder names (contains +=% and others)
                    dest_folder = os.path.dirname(src_path.replace(src, dest)) \
                        .replace("+", "") \
                        .replace("=", "") \
                        .replace("%", "")
                    if not os.path.exists(dest_folder):
                        os.makedirs(dest_folder)

                    dest_path = os.path.join(dest_folder, filename)
                    if os.path.isfile(dest_path):
                        src_sha1 = file_utils.calculatesha1(src_path)
                        dest_sha1 = file_utils.calculatesha1(dest_path)
                        skip = src_sha1 == dest_sha1
                    else:
                        skip = False

                    if skip:
                        # print("File already exists. Skipped: '%s'" % dest_path)
                        skipped_files.append(dest_path)
                        skipped += 1
                    else:
                        print("copy new file: '%s'" % src_path)
                        shutil.copyfile(src_path, dest_path)
                        copied += 1
                        copied_files.append(dest_path)
                except OSError as err:
                    print(err)

    print("CopiedFiles: {}, skipped_files: {}".format(copied, skipped))
    current_date = str(datetime.datetime.now()).replace(' ', '_')
    file_utils.printIterableToFile(
        os.path.join(work_dir, "copiedFiles_{}.txt".format(current_date)),
        copied_files)
    file_utils.printIterableToFile(
        os.path.join(work_dir, "skippedFiles_{}.txt".format(current_date)),
        skipped_files)


if __name__ == '__main__':
    main(sys.argv[1:])
