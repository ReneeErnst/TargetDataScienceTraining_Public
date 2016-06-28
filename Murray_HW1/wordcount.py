# Note that this code works even when reducer is not set to 1
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from mrjob.job import MRJob
import re, string

class MRJobWordCount(MRJob):

    def mapper(self, _, line):
        # Creates regular expression to modify punctuation characters so that they will not mess up regex
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        # Splits the lines by the first 2 tabs. Keeps everything after the second tab. 
        token = line.strip().split('\t', 2)[-1]
        # Uses the regex above to replace puctuation with a ' ' and puts all words in lower case
        token = regex.sub(' ', token.lower())
        # Converts all white space that is not a ' ' into a ' ', including duplicate spaces
        token = re.sub( '\s+', ' ', token )
        
        # Creates a list of the words
        words = token.split()

        # for loop that results in only words greater than length 1 being yielded
        for word in words:
            if len(word) > 1:
                yield (word, 1)
   
    # Creats sum by word for each mapper - like a per-mapper reducer
    def combiner(self, word, counts):
        yield (word, sum(counts))

    # Creates an empty dictionary called results, then sums 
    def reducer(self, word, counts):
        yield (word, sum(counts))
    
if __name__ == '__main__':
    MRJobWordCount.run()