#!/usr/bin/python3
import argparse
import datetime
import fnmatch
import json
import os
import sys

# DB
import psycopg2

from sourcesToHtmlConversion.file_utils import calculatesha1, normalizeFilePath


# import logging
# from multiprocessing_logging import install_mp_handler # for logging from multiprocessing.pool processes


def main(args):
    parser = argparse.ArgumentParser(
        description='Compare different runs on the conversion pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-work_dir', action="store", default="./conversion", help="working directory")

    params = parser.parse_args(args)

    work_dir = normalizeFilePath(params.work_dir)

    print("""Compare analysis stage params: 
               \twork_dir={}"""
          .format(work_dir))

    # result of analysis folder
    converting_summary_folder = os.path.join(work_dir, 'convertingSummary')

    # previous run data
    last = find_last_checksums_file(converting_summary_folder)
    last_data = None
    if last is None:
        print("no previous run summary was found")
    else:
        print("last checksums file: {}".format(last))
        last_data = load_from_disk(last)

    # current run data
    current_data = analyze_folder(os.path.join(work_dir, 'converted'))
    save_to_disk(current_data, os.path.join(converting_summary_folder,
                                            'checksums_{}.json'.format(str(datetime.datetime.now()).replace(' ', '_'))))

    if last is None:
        sys.exit()

    # compute diff and report
    cdiff = compute_diff(current_data, last_data)
    if not cdiff:
        print("diff is empty")
        return None

    with open(os.path.join(work_dir, 'mdb.json')) as f:
        mdb_conf = json.load(f)
    connection = psycopg2.connect(**mdb_conf)

    cur = connection.cursor()
    cur.execute("select uid, pattern from sources;")
    uid_pattern_map = dict(cur.fetchall())
    cur.close()
    connection.close()

    cdiff.sort(key=lambda x: uid_pattern_map.get(x.split('_')[0]) or "")

    with open(os.path.join(converting_summary_folder,
                           'compare_diff_{}.csv'.format(str(datetime.datetime.now()).replace(' ', '_'))), 'w') as out:
        for x in cdiff:
            uid, lang, fmt, status, name = x.split('_', 4)
            msg = ','.join([uid_pattern_map[uid] or "missing_pattern", uid, lang, fmt, status, name])
            print(msg)
            print(msg, file=out)

    return None


def load_from_disk(path):
    with open(path, "r") as f:
        return json.load(f)


def save_to_disk(data, path):
    with open(path, "w") as f:
        json.dump(data, f)


def analyze_folder(path):
    data = {}
    for root, dirs, files in os.walk(path):
        for filename in fnmatch.filter(files, '*.json'):
            # try
            src_path = os.path.abspath(os.path.join(root, filename))
            with open(src_path) as f:
                data[os.path.basename(root)] = json.load(f)

    for k, v in data.items():
        for lang, files in v.items():
            for fmt, f in files.items():
                src_sha1 = calculatesha1(os.path.abspath(os.path.join(path, k, f)))
                # print('{}\t{}\t{}\t{}\t{}'.format(k, lang, fmt, f, src_sha1))
                data[k][lang][fmt] = {
                    'name': f,
                    'sha1': src_sha1,
                }

    return data


def compute_diff(a, b):
    diff = []
    for k, v in a.items():
        b_source = b.pop(k, None)
        if v == b_source:
            continue
        elif b_source is None:
            for lang, files in v.items():
                for fmt, f in files.items():
                    diff.append('_'.join([k, lang, fmt, 'source-removed', f['name']]))
            continue

        for lang, files in v.items():
            b_source_lang = b_source.pop(lang, None)
            if files == b_source:
                continue
            elif b_source_lang is None:
                for fmt, f in files.items():
                    diff.append('_'.join([k, lang, fmt, 'language-removed', f['name']]))
                continue

            for fmt, f in files.items():
                b_source_lang_fmt = b_source_lang.pop(fmt, None)
                if f == b_source_lang_fmt:
                    continue
                elif b_source_lang_fmt is None:
                    diff.append('_'.join([k, lang, fmt, 'format-removed', f['name']]))
                    continue
                elif f['name'] == b_source_lang_fmt['name']:
                    diff.append('_'.join([k, lang, fmt, 'sha1-changed', f['name']]))
                elif f['sha1'] == b_source_lang_fmt['sha1']:
                    diff.append('_'.join([k, lang, fmt, 'name-changed', f['name']]))
                else:
                    diff.append('_'.join([k, lang, fmt, 'name-and-sha1-changed', f['name']]))

            for fmt, f in b_source_lang.items():
                diff.append('_'.join([k, lang, fmt, 'format-added', f['name']]))

        for lang, files in b_source.items():
            for fmt, f in b_source[lang].items():
                diff.append('_'.join([k, lang, fmt, 'language-added', f['name']]))

    for k, v in b.items():
        for lang, files in v.items():
            for fmt, f in v[lang].items():
                diff.append('_'.join([k, lang, fmt, 'source-added', f['name']]))

    return diff


def find_last_checksums_file(folder):
    last_file = None
    last_file_mtime = 0
    for root, dirs, files in os.walk(folder):
        for filename in fnmatch.filter(files, 'checksums*.json'):
            path = os.path.abspath(os.path.join(root, filename))
            stat_res = os.stat(path)
            if stat_res.st_mtime > last_file_mtime:
                last_file_mtime = stat_res.st_mtime
                last_file = path

    return last_file


if __name__ == "__main__":
    main(sys.argv[1:])

    # folder_data = analyze_folder('/home/edos/projects/source-conversion/conversion/converted')
    # save_to_disk(folder_data, '/home/edos/projects/source-conversion/folder_db.json')
    # data_from_disk = load_from_disk('/home/edos/projects/source-conversion/folder_db.json')
    # # data_from_disk.popitem()
    # # folder_data.popitem()
    # # data_from_disk['kVVhuoxu'].popitem()
    # # folder_data['kVVhuoxu'].popitem()
    # # data_from_disk['kVVhuoxu']['he'].popitem()
    # # folder_data['kVVhuoxu']['he'].popitem()
    # # data_from_disk['kVVhuoxu']['he']['html']['sha1'] = 'something'
    # # folder_data['kVVhuoxu']['he']['html']['sha1'] = 'something'
    # folder_data['kVVhuoxu']['he']['html']['name'] = 'something'
    # cdiff = compute_diff(folder_data, data_from_disk)
    # for x in cdiff:
    #     print(x)
