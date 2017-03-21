# -*- coding: utf-8 -*-


import json
from datetime import datetime
from pymongo import MongoClient
from IPython import embed
import os
import random
import time

from multiprocessing import Pool, Process

import Queue
import argparse

parser = argparse.ArgumentParser(description='Phone Number Blacklist.')
parser.add_argument('flag', type=int, help='flag')
parser.add_argument('amount', type=int, help='amount')
args = parser.parse_args()

queue = Queue.Queue()


import sys
reload(sys)
sys.setdefaultencoding('utf-8')


'''连接到mongodb数据库'''
# 使用默认链接
client = MongoClient(connect=False)

# 数据库名为blacklist
db = client.paws

bear = db.bear


def line2json(line):
    item = {}
    if '\x1a\xc5' not in line:
        try:
            data_ = line.split('\x01')
            item['event_time'] = '{} {}'.format(data_[0], data_[2])
            item['event_ip'] = data_[6]
            item['event_baiduid'] = data_[4]
            item['event_userid'] = data_[3]
            item['event_click_pos'] = data_[8]
            item['ps_subclick_pos'] = data_[9].decode("unicode_escape")
            item['event_click_target_url'] = data_[10].decode("unicode_escape").strip()
            item['event_query'] = data_[5]
            item['event_useragent'] =data_[7].replace('%20', ' ').decode("unicode_escape").strip()
        except:
            print line
            print  data_[7]

            pass
    return item


def run(i, lines):
    json_list = []
    print 'Run task {}...'.format(os.getpid())
    start = time.time()

    while lines:
        #line = lines.pop(random.randrange(len(lines)))#随机拿出一行
        line = lines.pop()#从最后一行开始
        json = line2json(line)
        if json:
            json_list.append(json)
        if len(json_list) == 400000:
            print 'file: {} surplus {} lines'.format(i, len(lines))
            bear.insert_many(json_list)
            json_list = []

    if json_list:
        bear.insert_many(json_list)
        json_list = []
    print 'save over.'
    end = time.time()
    print 'Task runs {} seconds.'.format(end - start)
    sys.exit()


if __name__ == '__main__':
    file_list = []
    for i in xrange(args.flag, args.flag + args.amount):
        file_list.append(i)

    print 'Parent process %s.' % os.getpid()
    print file_list
    files = []
    for i in file_list:
        file_ = '/data/file_name/0000{}_0'.format(i)
        if os.path.isfile(file_):
            files.append(file_)
    print len(files)
    print files

    for i in files:
        with open(i) as f:
            lines = f.readlines()
            lines.reverse()
            print '{} 共有 {} 行数据.'.format(i, len(lines));
        p = Process(target=run, args=(i, lines,))
        p.start()
    print 'Waiting for all subprocesses done...'
    p.join()
    print 'All subprocesses done.'
