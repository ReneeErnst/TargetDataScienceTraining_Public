
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, string

class IssueWordCountSort(MRJob):
    
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
        # converting bytes to utf8 string - deals non-basic/keyboard characters
        line = line.decode('utf8', 'ignore')
        
        complaint_id, complaint_type, sub_product, issue, _ = line.split(',', 4)
        
        # Creates regular expression to modify punctuation characters so that they will not mess up regex
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        # Uses the regex above to replace puctuation with a ' ' and puts all words in lower case
        issue = regex.sub(' ', issue.lower())
        # Converts all white space that is not a ' ' into a ' ', including duplicate spaces
        issue = re.sub( '\s+', ' ', issue)
        
        # Creates a list of the words
        issue = issue.split()

        del complaint_type, sub_product, _
        
        # If it can't convert complaint id to an int, 
        # then skip that row of data (i.e., skip the headder in the csv)
        try:
            int(complaint_id)
        except: 
            return
        
        for word in issue:
            yield (word, 1)
            
    def combiner(self, word, counts):
        yield (word, sum(counts))
        
    def reducer(self, word, counts):
        count = sum(counts)
        yield (word, count)
        
    def mapper_sort(self, word, count):
        # take a (very) large number and minus the count to 
        # reverse the way MapReduce normally sorts (smallest to largest)
        value = 1e6 - count
        yield ((value, word), None)
        
    def reducer_sort(self, value_word, _):
        # this is taking care of ties. Map reduce will order on word second if there is a tie. 
        value, word = value_word
        yield (word, int(abs(value - 1e6)))

if __name__ == '__main__':
    IssueWordCountSort.run()