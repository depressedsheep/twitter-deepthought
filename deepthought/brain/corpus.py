import os
import cPickle as pickle
import logging

from gensim import corpora


class blobbify(object):
    def __init__(self, key):
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
        logging.info("Initialised brain.corpus.blobbify()")
        self.key = key

        self.dirs = {
            'dict': os.path.join('thinking', 'braindict', self.key),
            'dump': os.path.join('thinking', 'braindump', self.key),
            'corp': os.path.join('thinking', 'braincorp', self.key + '.mm')
        }

    def gen(self):
        self.f_dict = open(self.dirs['dict'], 'rb+')  # dictionary
        self.f_text = open(self.dirs['dump'], 'rb+')
        self.dict = pickle.load(self.f_dict)
        corpora.MmCorpus.serialize(self.dirs['corp'], [self.dict.doc2bow(line.split(' ')) for line in self.f_text])

        logging.info('MMCorpus file created at ' + self.dirs['corp'])
        self.f_text.close()
        self.f_dict.close()