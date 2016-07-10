# Create word counter
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
import re, string
 
class IssueWordCount(MRJob):
    
    def mapper(self, _, line):
        
        self.increment_counter('group', 'Num_mapper_calls', 1)
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
        
    def reducer(self, word, counts):
        self.increment_counter('group', 'Num_reducer_calls', 1)
        yield (word, sum(counts))

if __name__ == '__main__':
    IssueWordCount.run()