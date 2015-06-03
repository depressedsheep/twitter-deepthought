from gensim import corpora, models, similarities
import logging
import os
from nltk.corpus import stopwords
import numpy as np
import json
import re
import cPickle as pickle
import brain.dicter, brain.cleaner, brain.corpus
import base64
import logging
import multiprocessing
import datetime
from brain.config import boto_access, boto_secret
from boto.s3.connection import Location, S3Connection
from boto.s3.key import Key
import zipfile

stop = stopwords.words('english')


def main():
    t_list = [
        '31-03-2015_02',
        '31-03-2015_03'
    ]
    t_list.sort(key=lambda x: gen_dto(x))
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO, filename='[LOG]')

    d = deepthought()
    d.jumpstart('test')
    # d.start()
    #d.create_dict()
    #d.create_corpus()
    #d.create_tfidf()


def gen_date(parameters):
    """ Parameters is a dictionary with the below tuples, printing the possible filenames for the hours in the time range.
    :params
        'months': (11,12)
        'days': (12, 19)
        'years': (2015, 2015)
        'hours': (15,19) """
    a = datetime.datetime(parameters['years'][0], parameters['months'][0], parameters['days'][0],
                          parameters['hours'][0])
    b = datetime.datetime(parameters['years'][1], parameters['months'][1], parameters['days'][1],
                          parameters['hours'][1])
    c = b - a
    no_h = divmod(c.days * 86400 + c.seconds, 3600)[0]
    date_list = [b - datetime.timedelta(hours=x) for x in xrange(0, no_h)]
    date_list = map(lambda x: '{y}-{m}-{d}_{h}'.format(y=x.strftime('%Y'), m=x.strftime('%m'), d=x.strftime('%d'),
                                                       h=x.strftime('%H')), date_list)
    return date_list


def gen_dto(date):
    # e.g. '31-03-2015_02'
    d = []
    d.append(date[6:10])  # y
    d.append(date[3:5])  # m
    d.append(date[:2])  # d
    d.append(date[-2:])  # h
    d = map(lambda x: int(x), d)
    t = datetime.datetime(d[0], d[1], d[2], d[3])
    return t


class deepthought(object):
    """
    Primary analysis module, downloading from Amazon S3, applying Natural Language Processing to it.    
    Working directory is moved to root directory out of convenience.
    """

    def __init__(self):
        self.t_stop = ['rt', '#', 'http', '@']
        os.chdir("..")

    def jumpstart(self, dir, fformat='raw'):
        """ Assumes you've already downloaded and extracted the files you want into a directory. 
        Also assumes various file formats: .json, .csv.
        I have no idea what these files are like tbh.
        :param
            dir: relative path of dir you want to use. 
            ctrl: Whether it is user controlled. If it is, it'll not try to download the file from S3"""

        logging.info("Current working directory: " + os.path.abspath(os.curdir))
        self.dirs = {
            'load': os.path.join(dir, 'thinking'),
            'dump': os.path.join(dir, 'thinking', 'braindump'),
            'corp': os.path.join(dir, 'thinking', 'braincorp'),
            'dict': os.path.join(dir, 'thinking', 'braindict'),
            'lsi': os.path.join(dir, 'thinking', 'brainlsi')
        }
        queue = multiprocessing.Queue()
        jobs = []
        for filename in os.listdir(dir):
            if os.path.isdir(filename):
                continue
            f = open(os.path.join(dir, filename), 'rb')
            ctrl = True
            p = multiprocessing.Process(target=self.fetch, args=(os.path.join(dir, filename), queue, fformat, ctrl))
            jobs.append(p)
            p.start()
            queue.get()
            p.join()

    def start(self, key_list):
        self.dirs = {
            'load': 'thinking',
            'dump': os.path.join('thinking', 'braindump'),
            'dict': os.path.join('thinking', 'braindict'),
            # the last three are kind of redundant, will clear soon. were meant for pickling.
            'corp': os.path.join('thinking', 'braincorp'),
            'tfidf': os.path.join('thinking', 'braintfidf'),
            'lsi': os.path.join('thinking', 'brainlsi')
        }

        self.key_list = key_list
        try:
            self.fname = self.get_hashbrown()
        except pickle.PickleError:
            logging.info("Hashbrown does not exist yet.")

        logging.info("Requesting " + str(self.key_list))
        logging.info("Current working directory: " + os.path.abspath(os.curdir))
        """ Use multiple processes to download each compressed file. Not sure if it's actually faster. """
        logging.info("Download started.")

        queue = multiprocessing.Queue()
        jobs = []
        for key in self.key_list:
            p = multiprocessing.Process(target=self.fetch, args=(key, queue,))
            jobs.append(p)
            p.start()
            queue.get()
            p.join()

    def fetch(self, key, queue, fformat, ctrl=False, ):
        # self.ensure_dir(self.dirs['load'])
        p = brain.cleaner.launch(queue)
        if not ctrl:
            p.load(key)
        p.sweep(key, fformat)

    def get_hashbrown(self):
        """ Get the generated hash for the selected file list. """
        try:
            f = pickle.load(open(os.path.join('thinking', 'hashbrowns'), 'rb'))
        except pickle.PickleError:
            raise

        return f['.'.join(self.key_list)]


    def create_dict(self):
        self.ensure_dir(self.dirs['dict'])
        d = brain.dicter.librarian(self.key_list)
        self.fname = d.cookhash()
        d.gen()

    def create_corpus(self):
        self.ensure_dir(self.dirs['corp'])
        c = brain.corpus.blobbify(self.fname)
        c.gen()

    def create_tfidf(self, force=False):
        logging.info("Attempting to create tf-idf model.")
        self.ensure_dir(self.dirs['tfidf'])
        if not os.path.exists(os.path.join(self.dirs['tfidf'], self.fname + '.tfidf_model')):
            logging.info("Tf-idf model does not yet exist for the current hash. Creating now.")
            self.tfidf_creator()
        else:
            if force == True:
                logging.info("Forced to create Tf-idf model.")
                self.tfidf_creator()
            else:
                logging.info("Tf-idf model already created. Set force to True to create again.")

    def tfidf_creator(self):
        logging.info("Attempting to convert current corpus to the Tf-idf model.")
        self.f_corp = open(os.path.join(self.dirs['corp'], self.fname + '.mm'), 'r')
        self.corpus = corpora.MmCorpus(self.f_corp)
        # print self.corpus

        self.tfidf = models.TfidfModel(corpus=self.corpus,
                                       dictionary=pickle.load(open(os.path.join(self.dirs['dict'], self.fname))))
        self.corpus_tfidf = self.tfidf[self.corpus]
        self.tfidf.save(os.path.join(self.dirs['tfidf'], self.fname + '.tfidf_model'))

        logging.info("Tf-idf model created, saved in tfidf_model format.")

    def create_lsi(self, force=False):
        #
        # Current approach: generate a seperate LSI / LSA for each time block, then compare over time
        # After initial development and for research purposes, consider collecting a few days worth of data, before creating a 'master' dictionary and 'corpus' to create a LSI/LSA model for topic-modelling
        # In that case, ensure memory efficiency (i.e. not loading large chunks of data into memory), and possibly use a different organisational structure
        # Currently not very sure, but I'm guessing you'd add an option into the vector generator function to use the 'master' dict, and then add into the LSA structure
        #
        self.ensure_dir(self.dirs['lsi'])
        if os.path.exists(os.path.join(self.dirs['lsi'], self.fname + '.lsi')):
            if force == True:
                logging.info("Forced to create LSI/LSA model again.")
                self.lsi_creator()
            else:
                logging.info("LSI/LSA model already created. Set force to True to create LSI model again.")
                pass
        else:
            self.lsi_creator()

    def lsi_creator(self, document_size=1000):
        self.f_corp = open(os.path.join(self.dirs['corp'], self.filename + '.mm'), 'r')
        self.corpus = corpora.MmCorpus(self.f_corp)

        self.tfidf = models.TfidfModel.load(os.path.join(self.dirs['tfidf'], self.fname + '.tfidf_model'))

        self.corpus_tfidf = self.tfidf[self.corpus]

        print self.tfidf
        self.dict = pickle.load(open(os.path.join(self.dirs['dict'], self.fname)))
        self.lsi = models.LsiModel(self.corpus_tfidf, id2word=self.dict, num_topics=200)
        self.corpus_lsi = self.lsi[self.corpus_tfidf]  # double wrapper over the original corpus
        self.lsi.print_topics(5)
        self.lsi.save(os.path.join(self.dirs['lsi'], self.fname + '.lsi'))
        logging.info("LSA model created.")

    def display_lsi(self, n=10):
        self.lsi = models.LsiModel.load(os.path.join(self.dirs['lsi'], self.fname + '.lsi'))
        self.lsi.print_debug(n)

    def ensure_dir(self, f):
        if not os.path.exists(f):
            os.makedirs(f)

    def print_list(self):
        print "Key list: "
        conn = S3Connection(boto_access, boto_secret)
        bucket = conn.get_bucket('twitter-deepthought')
        for z in bucket.list():
            print z.name


if __name__ == '__main__':
    main()
