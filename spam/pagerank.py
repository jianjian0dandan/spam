#-*- coding:utf-8 -*-

import re
import os
import sys
import shutil
import socket
from operator import add
from optparse import OptionParser
from pyspark import SparkContext

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def pagerank(iter_count, input_file, top_n):
    '''
    if not (iter_count and input_file and os.path.exists(input_file)):
        print 'error'
        return []
    '''
    prefix_name = '/mnt/mfs/'
    file_name = input_file.split('/')[-1]
    tmp_file_path = os.path.join("file://" + prefix_name, file_name )

    shutil.copy(input_file, prefix_name)
    sc = SparkContext(appName=file_name,master="mesos://219.224.135.48:5050")
    # sc = SparkContext(appName=input_file)

    lines = sc.textFile(tmp_file_path, 1)

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    ranks = links.map(lambda (url, neighbors): (url, 1.0))

    for iteration in xrange(int(iter_count)):
        contribs = links.join(ranks).flatMap(
            lambda (url, (urls, rank)): computeContribs(urls, rank))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    results_list = []

    for (link, rank) in ranks.collect():
        results_list.append((link, rank))
    if not results_list:
        return [], {}
    results_list = sorted(results_list, key=lambda result:result[1], reverse=True)

    all_uid_r = {}
    for uid, r in results_list:
        all_uid_r[uid] = r

    if len(results_list) > top_n:
        results_list = results_list[:top_n]

    sorted_uids = []
    # f = open("out.txt", "w")
    for uid, r in results_list:
        sorted_uids.append(uid)
        # print '%s\t%s\n' % (uid, r)
        # print >> f, '%s\t%s\n' % (uid, r)
    # f.close()
    # delete file
    os.remove(prefix_name + file_name)
    sc.stop()
    return sorted_uids, all_uid_r


if __name__ == "__main__":

    optparser = OptionParser()
    optparser.add_option('--input',dest='input_path', help='Input File Path', default=None, type='string')
    optparser.add_option('--iter_count',dest='iter_count',help='PageRank Iter Count', default=2, type='int')
    (options, args) = optparser.parse_args()
    input_file = options.input_path
    iter_count = options.iter_count

    # if not (iter_count and input_file and os.path.exists(input_file)):
    #     print iter_count, input_file, os.path.exists(input_file)
    #     print 'Usage: python pagerank.py --help'
    #     sys.exit()

    top_n = 500
    pagerank(iter_count, input_file, top_n)
    # for uid in pagerank(iter_count, input_file, top_n):
    #     print uid



