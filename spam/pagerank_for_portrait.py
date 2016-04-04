#-*- coding:utf-8 -*-

import re
import os
import sys
import shutil
import socket
from operator import add, mul
from optparse import OptionParser
from pyspark import SparkConf, SparkContext


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], (parts[1], int(parts[2]))

def parseNeighborsKeywords(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return (parts[0], parts[1])

def pagerank(iter_count, input_file, top_n, flag):
    '''
    if not (iter_count and input_file and os.path.exists(input_file)):
        print 'error'
        return []
    '''
    prefix_name = '/mnt/mfs/'
    file_name = input_file.split('/')[-1]
    tmp_file_path = os.path.join("file://" + prefix_name, file_name )

    shutil.copy(input_file, prefix_name)
    conf = SparkConf()
    if flag == 'keywords': #keywords
        conf = conf.set('spark.ui.port', 4041)
    sc = SparkContext(appName=file_name,master="mesos://219.224.134.213:5050", conf=conf)
    lines = sc.textFile(tmp_file_path, 1)
    
    if flag == 'keywords': #keywords
        rdd_for_reduce = lines.map(lambda urls: (parseNeighborsKeywords(urls), 1.0)).reduceByKey(add) # ((uid_a,uid_b), num)
        initials = rdd_for_reduce.map(lambda ((uid_a, uid_b), num): (uid_a, (uid_b, num))).cache() # (uid_a, (uid_b, num))
    else:  #all
        initials = lines.map(lambda urls: parseNeighbors(urls)).distinct().cache() # (uid_a,(uid_b, num))
    

    user_ranks = initials.map(lambda (url, neighbors): (url, neighbors[1])).reduceByKey(add) #(uid_a, num)
    extra_ranks = initials.values().reduceByKey(add).cache() #(uid_b, num)

    degrees = user_ranks.union(extra_ranks).reduceByKey(add).cache()    # (uid, degree)
 
    degrees_list = []
    degrees_list = degrees.sortBy(lambda x:x[1], False).collect()
    if len(degrees_list) > top_n:
        degrees_list = degrees_list[:top_n]
    
    all_uids = initials.flatMap(lambda (url, neighbors): [url, neighbors[0]]).distinct()
    all_uids_map = all_uids.flatMap(lambda x: [('global', x), (x, 'global')])
    global_links = all_uids_map.groupByKey()
    
    ini_links = initials.map(lambda (url, neighbors): (url, neighbors[0])).groupByKey() #(uid_a, [uid_b,uid_c])
    links = global_links.union(ini_links).cache()
    init_ranks = links.map(lambda (url, neighbors): (url, 1.0))
    ranks = extra_ranks.union(init_ranks).reduceByKey(mul).cache() #(uid, rank)
    
    for iteration in xrange(int(iter_count)):
        contribs = links.join(ranks).flatMap(
            lambda (url, (urls, rank)): computeContribs(urls, rank))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    results_list = []
    results_list = ranks.sortBy(lambda x:x[1], False).collect()

    #exclude global
    top_n += 1
    if len(results_list) > top_n:
        results_list = results_list[:top_n]

    f = open("degree.txt", "w")
    for uid, r in degrees_list:
        print >> f, '%s\t%s\n' % (uid, r)
    f.close()
    f = open("rank.txt", "w")
    for uid, r in results_list:
        print >> f, '%s\t%s\n' % (uid, r)
    f.close()
    # delete file
    #os.remove(prefix_name + file_name)
    sc.stop()
    return degrees_list, results_list

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



