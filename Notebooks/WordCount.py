from mrjob.job import MRJob
# from mrjob.step import MRStep
import re
 
WORD_RE = re.compile(r"[\w']+")
 
class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1
     
    def combiner(self, word, counts):
        yield word, sum(counts)

    #hello, (1,1,1,1,1,1): using a combiner? NO and YEs
    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    MRWordFreqCount.run()