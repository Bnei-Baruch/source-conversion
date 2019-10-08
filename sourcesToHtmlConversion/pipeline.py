#!/usr/bin/python3

import sys
import os
import argparse

from sourcesToHtmlConversion import copy_files, organize, convert, converted_analysis

from sourcesToHtmlConversion.file_utils import normalizeFilePath


def main(args):
    parser = argparse.ArgumentParser(description='Run whole pipeline (copy, convert, organize)',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # paths
    parser.add_argument('src', action="store", help="Sources folder (PATH/____beavoda/")
    parser.add_argument('-work_dir', action="store", default="./conversion", help="working directory")

    # patterns
    parser.add_argument('-copyPathPrefix', action="store", default="____beavoda",
                        help="Paths at mapping xlsx file has ____beavoda/ prefix (4.10.2017)")
    parser.add_argument('-copy_patterns', action="append", default=['*.doc', '*.docx'],
                        help="Patterns for source docs copy stage filtering")
    parser.add_argument('-organize_patterns', action="append", default=['*.doc', '*.docx'],
                        help="Patterns for files filter")

    # skips
    parser.add_argument('--skipCopy', action="store_true", default=False, help="Skip copy stage")
    parser.add_argument('--skipOrganize', action="store_true", default=False, help="Skip organize stage")
    parser.add_argument('--skipConvert', action="store_true", default=False, help="Skip convert stage")
    parser.add_argument('--skipCompare', action="store_true", default=False, help="Skip compare stage")

    params = parser.parse_args(args)

    src = normalizeFilePath(params.src)
    work_dir = normalizeFilePath(params.work_dir)
    copy_patterns = set(params.copy_patterns)
    organize_patterns = params.organize_patterns

    print("""Pipeline params: 
        \tsrc={}
        \twork_dir={}
        \tcopyPathPrefix={}
        \tcopy_patterns={}
        \torganize_patterns={}"""
          .format(src, work_dir, params.copyPathPrefix, copy_patterns, organize_patterns))

    # Copy
    copied_folder = os.path.join(work_dir, 'copied')
    copied_folder_prefix = os.path.join(copied_folder, params.copyPathPrefix)
    if not params.skipCopy:
        copy_patterns_args = [None] * (len(copy_patterns) * 2)
        copy_patterns_args[::2] = ["-p" for t in copy_patterns]
        copy_patterns_args[1::2] = copy_patterns
        copy_files.main([src, copied_folder_prefix] + copy_patterns_args)
    else:
        print("Skip copy stage")

    # Organize
    if not params.skipOrganize:
        organize_patterns_args = [None] * (len(organize_patterns) * 2)
        organize_patterns_args[::2] = ["-p" for t in organize_patterns]
        organize_patterns_args[1::2] = organize_patterns
        organize.main([
                          copied_folder,
                          "-work_dir", work_dir,
                          "--clean"
                      ] + organize_patterns_args)
    else:
        print("Skip organize stage")

    # Convert
    if not params.skipConvert:
        convert.main([os.path.join(work_dir, 'organized'), "-work_dir", work_dir])
    else:
        print("Skip convert stage")

    # Compare
    if not params.skipCompare:
        converted_analysis.main(["-work_dir", work_dir])
    else:
        print("Skip compare stage")


if __name__ == '__main__':
    main(sys.argv[1:])
