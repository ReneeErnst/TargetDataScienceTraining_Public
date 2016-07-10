# Create word counter
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
import re
 
WORD_RE = re.compile(r"[\w']+")
 
class WordCounter(MRJob):
    def mapper(self, _, line):
        self.increment_counter('group', 'Num_mapper_calls', 1)
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def reducer(self, word, counts):
        self.increment_counter('group', 'Num_reducer_calls', 1)
        yield word, sum(counts)

if __name__ == '__main__':
    WordCount.run()