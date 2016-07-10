
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, string
from collections import Counter

stored_data = {}


class ShoppingStripesTop50(MRJob):
    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            ),
            MRStep(
                mapper=self.mapper_sort,
                reducer=self.reducer_sort
            )
        ]
    
    def mapper(self, _, line):
        self.increment_counter('group', 'Num_mapper_calls', 1)
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
            counts = Counter()
            for item_2 in basket[index + 1:]:
                counts[item_2] += 1
                
            # yielding a dictionary with the first item as the key
            # and the value as a dictionary with the other items that
            # occured with it and how many times they occured
            yield(item_1, counts)
            
        yield ("TOTAL_COUNT", {'TOTAL_COUNT': 1})

    def combiner(self, item, counters):
        self.increment_counter('group', 'Num_combiner_calls', 1)
        counts = Counter()
        for c in counters: 
            for item_2, count in c.items():
                counts[item_2] += count
                
        yield (item, counts)
        
    def reducer(self, item, counters):
        self.increment_counter('group', 'Num_reducer_calls', 1)
        counts = Counter()
        for c in counters: 
            for item_2, count in c.items():
                counts[item_2] += count
        
        if item == "TOTAL_COUNT":
            stored_data[item] = counts[item]
            return
        
        for item_2, count in counts.items():
            if count > 99:
                yield ((item, item_2), count)
        
    def mapper_sort(self, item_pair, count):
        self.increment_counter('group', 'Num_mapper_sort_calls', 1)
        # take a (very) large number and minus the count to 
        # reverse the way MapReduce normally sorts (smallest to largest)
        value = 1e6 - count
        yield ((value, item_pair), None)        
        
    def reducer_sort(self, value_item_pair, _):
        self.increment_counter('group', 'Num_reducer_sort_calls', 1)
        # this is taking care of ties. Map reduce will order on word second if there is a tie. 
        value, item_pair = value_item_pair
        freq = int(abs(value - 1e6))
        relative_freq = (int(abs(value - 1e6)) / stored_data["TOTAL_COUNT"])
        freqs = (freq, relative_freq)
        yield (item_pair, freqs)

if __name__ == '__main__':
    ShoppingStripesTop50.run()