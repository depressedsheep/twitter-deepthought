import os
import cPickle as pickle
import logging
import multiprocessing
import datetime

from gensim import corpora, models
from nltk.corpus import stopwords
from boto.s3.connection import S3Connection
from boto.s3.key import Key

from brain.config import boto_access, boto_secret

import gzip, bz2, csv, json

import re, base64

stop = stopwords.words('english')

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
    date_list = map(lambda x: '{d}-{m}-{y}_{h}'.format(y=x.strftime('%Y'), m=x.strftime('%m'), d=x.strftime('%d'),                                                       h=x.strftime('%H')), date_list)
    return date_list

def main():
    a = deepthought()
    #a.nlpstuff()
    keylist = gen_date({'months':(4,5), 'days':(1,20), 'years':(2015,2015),'hours':(0,23)})
    #a.download(keylist,'.gz')
    a.nlpstuff(load = True)

class deepthought():
    def __init__(self,**kwargs):
        print "I'm in {}".format(os.path.abspath('.'))
        self.checks = {
        's3':False
        }
        self.dirs = {
        'downloads':os.path.join('.','thinking','downloads'),
        'raw':os.path.join('.','thinking','raw'),
        'dict':os.path.join('.','thinking','dict'),
        'corpus':os.path.join('.','thinking','corp'),
        'tfidf':os.path.join('.','thinking','tfidf'),
        'lsi':os.path.join('.','thinking','lsi')
        }
    def check_dirs(self):
        for (k,v) in self.dirs.items():
            if not os.path.exists(v):
                os.makedirs(v)
    def connect(self):
        # Initalises Amazon S3 service
        self.checks['s3'] = True
        self.conn = S3Connection(boto_access, boto_secret)
        self.bucket = self.conn.get_bucket('twitter-deepthought')
        self.k = Key(self.bucket)
    def list_keys(self):
        bucket_keys = []
        f = open('.keys','w')
        if not self.checks['s3']:
            self.connect()
            self.checks['s3'] = True
        for key in self.bucket.list():
            bucket_keys.append(key.name)
            f.write(key.name + '\n')
        for key in bucket_keys:
            print key
    def download(self, key_list, filetype):
        self.check_dirs()
        for a in xrange(5):
            print a
        try:
            pk = (pickle.load(open('.presentkeys','rb')))
        except EOFError:
            pk = set()
            pass
        if not self.checks['s3']:
            self.connect()
        for key in key_list:
            try:
                print key
                fp = os.path.join(self.dirs['downloads'],key+filetype)
                self.k.key = key
                if os.path.exists(fp):
                    print "{} already exists".format(key+filetype)
                else:
                    self.k.get_contents_to_filename(fp)
                pk.add(key+filetype)
            except:
                print "{} not found.".format(key+filetype)
                pass
        pickle.dump(pk,open('.presentkeys','w'))
        if filetype == '.gz':
            g = open(os.path.join(self.dirs['dict'],'merged'),'w')
            for key in key_list:
                if not (key+filetype) in pk:
                    continue
                print os.path.join(self.dirs['downloads'],key+filetype)
                print os.path.exists(os.path.join(self.dirs['downloads'],key+filetype))

                try:
                    f = gzip.open(os.path.join(self.dirs['downloads'],key+filetype),'rb')

                    for line in f:
                        tweet = json.loads(line)
                        text = tweet['text']
                        text = cleaner(text)
                        g.write(' '.join(text).encode('ascii','ignore') + '\n')
                except IOError:
                    print "ioerror"

            #os.remove(os.path.join(self.dirs['downloads'], key+filetype))
            g.close()
    def nlpstuff(self, load = False):
        self.check_dirs()

        if load:
            dict = pickle.load(open(os.path.join(self.dirs['dict'],'dict'),'rb'))
            corpus = corpora.MmCorpus(os.path.join(self.dirs['corpus'],'corp.mm'))
            tfidf = models.TfidfModel.load(os.path.join(self.dirs['tfidf'], 'tfidfmodel'))
        else:
            f = open(os.path.join(self.dirs['dict'],'merged'),'r')
            dict = corpora.Dictionary(line[:-1].lower().split() for line in f)
            once_ids = [tokenid for tokenid, docfreq in dict.iteritems() if docfreq == 1]
            dict.filter_tokens(once_ids)
            dict.compactify()
            pickle.dump(dict, open(os.path.join(self.dirs['dict'],'dict'), 'w'))
            f.close()
            f = open(os.path.join(self.dirs['dict'],'merged'),'r')
            corpus = [dict.doc2bow(line.split(' ')) for line in f]
            corpora.MmCorpus.serialize(os.path.join(self.dirs['corpus'], 'corp.mm'),corpus)
            tfidf = models.TfidfModel(corpus = corpus, dictionary=dict)
            corpus_tfidf = tfidf[corpus]
            tfidf.save(os.path.join(self.dirs['tfidf'], 'tfidfmodel'))

        #lsi = models.LsiModel(corpus_tfidf, id2word = dict, num_topics=200)
        #corpus_lsi = lsi[corpus_tfidf]
        #lsi.print_topics(5)

def cleaner(text):
   	def strip_emojis(tl):
		myre = re.compile(
            u'['u'\U0001f300-\U0001ffff'u'\U0001f600-\U0001f64f'u'\U0001f680-\U0001f6ff'u'\u2600-\u26ff\u2700-\u27bf]+',
            re.UNICODE)
		return myre.sub('', ' '.join(tl)).split(' ')
	def strip_escape(text):
		while True:
			if text[:1] == '\n':
				text = text[1:]
			else:
				break
		return text
	def strip_others(text):
		t_stop = ['rt','#','http','@']
		for a in t_stop:
			if a in text:
				return False
		return True

	tl = unicode(text.lower()).split(' ')
	tl = strip_emojis(tl)
   	tl = filter(lambda w: (not w in stop), tl)
   	tl = map(strip_escape, tl)
   	tl = filter(strip_others, tl)
   	return tl

if __name__ == '__main__':
    main()
