
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, string


class ShoppingPairs(MRJob):
    
    def mapper(self, _, line):
        
        # Creates regular expression to modify punctuation characters so that they will not mess up regex
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        # Uses the regex above to replace puctuation with a ' ' and puts all words in lower case
        basket = regex.sub(' ', line.lower())
        # Converts all white space that is not a ' ' into a ' ', including duplicate spaces
        basket = re.sub( '\s+', ' ', basket)
        
        # Creates a list of items in each basket
        # Set function removes any duplicates
        basket = list(set(basket.split()))
        # Sorts everything in the baseket alphabetically so that we know
        # that all the item pairs will be in alphabetical order
        basket.sort()
        
        for index, item_1 in enumerate(basket):
            for item_2 in basket[index + 1:]:
                yield ((item_1, item_2), 1)
            
    def combiner(self, item_pair, counts):
        yield (item_pair, sum(counts))
        
    def reducer(self, item_pair, counts):
        count = sum(counts)
        # Only yield pairs that occurr at least 100 times
        if count > 99:
            yield (item_pair, count)

if __name__ == '__main__':
    ShoppingPairs.run()