#!/usr/bin/env python3

import os
import sys
import boto3
import mmap

from collections import namedtuple
from argparse import ArgumentParser
from threading import Thread
from queue import Queue
from multiprocessing import cpu_count
from botocore.utils import calculate_tree_hash

MIN_PARTS = 1
MAX_PARTS = 10000

MIN_PART_SIZE = 1 << 20  # 1 MiB
MAX_PART_SIZE = 1 << 32  # 4 GiB

Task = namedtuple('Task', ['uploadId', 'range', 'data', 'last', 'totalsize', 'whole_file_treehash'])


class Uploader(Thread):
    def __init__(self, q, glacier, vault):
        self.q = q
        self.glacier = glacier
        self.vault = vault
        super(Uploader, self).__init__()

    def run(self):
        while True:
            d = self.q.get()
            if d is None:
                break
            print("Uploading part %s" % d.range)
            self.glacier.upload_multipart_part(
                vaultName=self.vault,
                uploadId=d.uploadId,
                range=d.range,
                body=d.data,
            )
            if d.last:
                print("Completing multipart upload")
                self.glacier.complete_multipart_upload(
                    vaultName=self.vault,
                    uploadId=d.uploadId,
                    archiveSize=str(d.totalsize),
                    checksum=d.whole_file_treehash,
                )


def main():
    parser = ArgumentParser()
    parser.add_argument('-a', '--aws_profile', help='aws credentials file profile name')
    parser.add_argument('-p', '--parts', help='number of chunks to split files into', type=int, default=1000)
    parser.add_argument('-o', '--overwrite', help='Overwrite behavior',
                        choices=['never', 'older', 'footgun'], default='older')
    parser.add_argument('-t', '--threads',
                        help='Number of threads to push in parallel ' \
                        '(default numprocs + 1)', default=cpu_count() + 1)
    parser.add_argument('-V', '--vault', help='Name of glacier vault', required=True)
    parser.add_argument('FILE', nargs='+')

    args = parser.parse_args(sys.argv[1:])

    Q = Queue()
    G = None
    if args.aws_profile:
        s = boto3.Session(profile_name=args.aws_profile)
        G = s.client('glacier')
    else:
        G = boto3.client('glacier')

    threads = []
    for x in range(0, args.threads):
        t = Uploader(Q, G, args.vault)
        threads.append(t)
        t.start()

    for f in args.FILE:
        size = os.stat(f).st_size
        chunksize = None
        if size < (MIN_PART_SIZE * args.parts):
            chunksize = MIN_PART_SIZE
        else:
            chunksize = (size // args.parts)

        with open(f, 'rb') as fp:
            r = G.initiate_multipart_upload(
                vaultName=args.vault,
                archiveDescription=f,
                partSize=str(chunksize),
            )
            start = 0
            th = calculate_tree_hash(fp)
            fp.seek(0)
            fmap = mmap.mmap(fp.fileno(), 0, prot=mmap.PROT_READ)

            while True:
                last = False
                end = start + chunksize
                if end > len(fmap):
                    last = True
                    end = len(fmap)
                Q.put(Task(r['uploadId'],
                           "bytes %d-%d/*" % (start, end-1),
                           fmap[start:end], last, size, th))
                start = end
                if start >= size:
                    break
    for t in threads:
        Q.put(None)

    for t in threads:
        t.join()

