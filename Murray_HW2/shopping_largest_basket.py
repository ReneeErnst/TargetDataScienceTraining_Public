
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, string, uuid

class ShoppingLargestBasket(MRJob):
    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper
            ),
            MRStep(
                mapper=self.mapper_sort,
                reducer=self.reducer_sort
            )
        ]
    
    def mapper(self, _, line):
        
        # Creates regular expression to modify punctuation characters so that they will not mess up regex
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        # Uses the regex above to replace puctuation with a ' ' and puts all words in lower case
        basket = regex.sub(' ', line.lower())
        # Converts all white space that is not a ' ' into a ' ', including duplicate spaces
        basket = re.sub( '\s+', ' ', basket)
        
        # Creates a list of items in each basket
        basket = basket.split()
        basket_size = len(basket)
        
        # yielding a unique id to clean up output (don't need to output everything in the basket)
        yield (str(uuid.uuid4()), basket_size)
        
    def mapper_sort(self, basket, basket_size):
        # take a (very) large number and minus the count to 
        # reverse the way MapReduce normally sorts (smallest to largest)
        value = 1e6 - basket_size
        yield ((value, basket), None)
        
    def reducer_sort(self, value_basket, _):
        # this is taking care of ties. Map reduce will order on word second if there is a tie. 
        value, basket = value_basket
        yield ("Basket Size", int(abs(value - 1e6)))

if __name__ == '__main__':
    ShoppingLargestBasket.run()