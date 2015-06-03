from gensim import corpora, models, similarities
import os
import cPickle as pickle
import uuid
import logging


class librarian(object):
    def __init__(self, key_list):
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
        logging.info("Initialising dictionary creation, brain.dicter.librarian()...")
        logging.info("Assuming file has been cleaned for stopwords.")
        self.key_list = key_list
        self.uname = str(uuid.uuid4())
        self.fname = ''
        self.dirs = {  # 'dict':os.path.join(os.path.dirname(__file__) + '/../thinking/braindict',  self.fname),
                       #'dump':os.path.join(os.path.dirname(__file__) + '/../thinking/braindump', self.fname),
                       'in': os.path.join('thinking', 'braindump'),
                       'flib': os.path.join('thinking', 'hashbrowns')
                       }

    def update_dir(self):
        self.dirs['dict'] = os.path.join('thinking', 'braindict', self.fname)
        self.dirs['dump'] = os.path.join('thinking', 'braindump', self.fname)

        # print self.dirs

    #print self.fname
    def merge(self):
        logging.info("Merging Files now.")
        self.update_dir()
        self.f_text = open(self.dirs['dump'], 'w')
        for key in self.key_list:
            print key
            f = open(os.path.join(self.dirs['in'], key), 'r')
            for line in f:
                self.f_text.write(line)
        logging.info("Files merged.")

    def cookhash(self):
        food = {}
        cake = '.'.join(self.key_list)
        food[cake] = self.uname
        if not os.path.exists(self.dirs['flib']):
            logging.info("Cooking a new hashbrown.")
            #print cake
            self.fname = self.uname
            pickle.dump(food, open(self.dirs['flib'], 'w'))
            self.merge()

        else:  #hash file already exists
            cfood = pickle.load(open(self.dirs['flib'], 'r'))
            print cfood
            if cake in cfood:
                logging.info("Hashbrown already exists.")
                self.fname = cfood[cake]
            else:
                logging.info("Hashbrown exists, but hash doesn't.")
                cfood[cake] = self.uname
                self.fname = self.uname
                pickle.dump(cfood, open(self.dirs['flib'], 'w'))
                self.merge()

        logging.info("Hashbrown done.")
        return self.fname


    def gen(self):
        self.dirs['dict'] = os.path.join('thinking', 'braindict', self.fname)
        self.dirs['dump'] = os.path.join('thinking', 'braindump', self.fname)

        self.f_dict = open(self.dirs['dict'], 'w')
        self.f_text = open(self.dirs['dump'], 'r')

        self.dict = corpora.Dictionary(line[:-1].lower().split() for line in self.f_text)
        once_ids = [tokenid for tokenid, docfreq in self.dict.iteritems() if docfreq == 1]
        self.dict.filter_tokens(once_ids)
        self.dict.compactify()

        pickle.dump(self.dict, self.f_dict)  #this is a dump of the dictionary

        self.f_text.close()
        self.f_dict.close()

        logging.info("Dict file created at " + self.dirs['dict'])
