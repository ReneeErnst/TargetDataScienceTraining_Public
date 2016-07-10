
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, string


class ShoppingUnique(MRJob):
    
    def mapper(self, _, line):
        
        # Creates regular expression to modify punctuation characters so that they will not mess up regex
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        # Uses the regex above to replace puctuation with a ' ' and puts all words in lower case
        basket = regex.sub(' ', line.lower())
        # Converts all white space that is not a ' ' into a ' ', including duplicate spaces
        basket = re.sub( '\s+', ' ', basket)
        
        # Creates a list of items in each basket
        basket = basket.split()
        
        for item in basket:
            yield (item, 1)
            
    def combiner(self, item, counts):
        yield (item, sum(counts))
        
    def reducer(self, item, counts):
        # This counter will give me the number of unique words
        # Reducer runs once for each key, each unique word = 1 key
        self.increment_counter('group', 'Num_reducer_calls/Unique Items', 1)
        count = sum(counts)
        yield (item, count)

if __name__ == '__main__':
    ShoppingUnique.run()