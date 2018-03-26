#!/usr/bin/env python3

import os
import sys
import boto3
import yaml

from boto3.s3.transfer import TransferConfig
from functools import partial
from collections import namedtuple
from argparse import ArgumentParser
from multiprocessing import cpu_count
from titanic import progbar


sofar = {}
def cb(f, sz, sent):
    try:
        sofar[f] += sent
    except KeyError:
        sofar[f] = sent
    progbar.update(sz, sofar[f])


def main():
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', help='config file', default=os.path.expanduser("~/.titanic.yml"))
    parser.add_argument('-o', '--overwrite', help='Overwrite behavior',
                        choices=['never', 'older', 'footgun'], default='older')
    parser.add_argument('-t', '--threads',
                        help='Number of threads to push in parallel ' \
                        '(default numprocs + 1)', default=cpu_count() + 1)
    parser.add_argument('FILE', nargs='+')

    args = parser.parse_args(sys.argv[1:])

    config = {}
    with open(args.config, 'r') as fp:
        config = yaml.load(fp)

    S = None
    session = boto3.Session()
    S = session.client('s3', region_name=config['region'],
                       endpoint_url='https://%s.digitaloceanspaces.com' % config['region'],
                       aws_access_key_id=config['access_key'],
                       aws_secret_access_key=config['secret_key'])

    transfer_config = TransferConfig(args.threads,
                                     use_threads=True)

    for f in args.FILE:
        size = os.stat(f).st_size
        progress = partial(cb, f, size)
        key = f[f.rfind('/')+1:]
        print("Uploading %s to do://%s/%s" % (f, config['bucket'], key))
        S.upload_file(f, config['bucket'], key, Callback=progress, Config=transfer_config)
        progbar.finish()
