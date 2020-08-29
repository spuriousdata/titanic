#!/usr/bin/env python3

import os
import sys
import boto3
import yaml
import stat
import hashlib
import tqdm

from boto3.s3.transfer import TransferConfig
from datetime import datetime as dt, timezone as tz
from argparse import ArgumentParser
from multiprocessing import cpu_count


CHUNKSIZES = [1 << i for i in range(20, 33)]


def main():
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', help='config file', default=os.path.expanduser("~/.titanic.yml"))
    parser.add_argument('-o', '--overwrite', help='Overwrite behavior',
                        choices=['never', 'older', 'checksum', 'footgun'], default='older')
    parser.add_argument('-t', '--threads', type=int,
                        help='Number of threads to push in parallel ' \
                        '(default multiprocessing.cpu_count() * 5)', default=cpu_count() * 5)
    parser.add_argument('-j', '--justprint', help="Don't actually upload anything", default=False, action='store_true')
    parser.add_argument('-s', '--skip', help='Skip this directory, supply multiple times', action='append')
    parser.add_argument('FILE', nargs='+', help="files or directories")

    args = parser.parse_args(sys.argv[1:])

    config = {}
    with open(args.config, 'r') as fp:
        config = yaml.load(fp)

    config['overwrite'] = args.overwrite
    config['justprint'] = args.justprint
    config['skip'] = args.skip

    S = None
    session = boto3.Session()
    S = session.client('s3', region_name=config['region'],
                       aws_access_key_id=config['access_key'],
                       aws_secret_access_key=config['secret_key'])

    transfer_config = TransferConfig(args.threads,
                                     use_threads=True)

    for f in args.FILE:
        walk(S, transfer_config, config, f)


def walk(S, transfer_config, config, fileish):
    s = os.stat(fileish)
    if fileish in config['skip']:
        return
    if stat.S_ISDIR(s.st_mode):
        for f in sorted(os.listdir(fileish)):
            walk(S, transfer_config, config, os.path.join(fileish, f))
    else:
        upload(S, transfer_config, config, s, fileish)


def verify(S, config, f, key, s):
    head = S.head_object(Bucket=config['bucket'], Key=key)
    etag = head['ETag'][1:-1] if head['ETag'][0] in ('"', "'") else head['ETag']
    if etag.find('-') == -1:
        return etag == checksum_file(f)
    else:
        cksm, count = etag.split('-')
        count = int(count)
        size = s.st_size
        ck = size / float(count)
        chunksize = [x for x in CHUNKSIZES if x > ck][0]
        s = bytes()
        with open(f, 'rb') as fp:
            while True:
                data = fp.read(chunksize)
                if len(data) == 0:
                    break
                s += hashlib.md5(data).digest()
            return cksm == hashlib.md5(s).hexdigest()


def checksum_file(f):
    with open(f, 'rb') as fp:
        return hashlib.md5(fp.read()).hexdigest()


def upload(S, transfer_config, config, s, f):
    key = f[1:] if f.startswith('/') else f
    if config.get('prefix', None):
        key = "%s/%s" % (config['prefix'], key)
    size = s.st_size
    try:
        if not config['overwrite'] == 'footgun':
            obj = S.head_object(Bucket=config['bucket'], Key=key)
            if config['overwrite'] == 'older':
                lmt = dt.fromtimestamp(s.st_mtime, tz.utc)
                if obj['LastModified'] > lmt:
                    #print("Skipping %s" % f)
                    return
            elif config['overwrite'] == 'checksum':
                if verify(S, config, f, key, s):
                    #print("Skipping %s" % f)
                    return
            elif config['overwrite'] == 'never':
                if obj.get('ETag', None):
                    #print("Skipping %s" % f)
                    return
    except Exception as e:
        pass
    if config['justprint']:
        print("Uploading %s" % f)
    else:
        print("[[Uploading %s]]" % f)
        p = tqdm.tqdm(total=size, unit_scale=1, smoothing=0, unit='B', unit_divisor=1024)
        S.upload_file(f, config['bucket'], key, Callback=p.update,
                    Config=transfer_config)
        p.close()
        if not verify(S, config, f, key, s):
            raise Exception("Verification failed for %s" % f)

if __name__ == '__main__':
    main()
