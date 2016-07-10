
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, string

stored_data = {}


class ShoppingPairsTop50(MRJob):
    
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
            for item_2 in basket[index + 1:]:
                yield ((item_1, item_2), 1)
        yield (("", "total_count"), 1)

    def combiner(self, item_pair, counts):
        self.increment_counter('group', 'Num_combiner_calls', 1)
        yield (item_pair, sum(counts))
        
    def reducer(self, item_pair, counts):
        self.increment_counter('group', 'Num_reducer_calls', 1)
        count = sum(counts)
        if len(item_pair[0]) < 1:
            stored_data[item_pair[1]] = count
            return
        if count > 99:
            yield (item_pair, count)

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
        relative_freq = (int(abs(value - 1e6)) / stored_data["total_count"])
        freqs = (freq, relative_freq)
        yield (item_pair, freqs)

if __name__ == '__main__':
    ShoppingPairsTop50.run()