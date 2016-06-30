
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob

"""
Calculate the Pr(term |class)
use the order inversion pattern (which uses secondary sort)

"""


class MrRelativeProbs(MRJob):
    DEFAULT_PROTOCOL = 'json'  #split records in key value pairs using TAB
    
    # Performs secondary sort on the word with in the class
    # as a result the Reducer receives records sort by class, and then by word (
    # remember we have a special word **Total
    SORT_VALUES = True

    def __init__(self, *args, **kwargs):
        super(MrRelativeProbs, self).__init__(*args, **kwargs)
        self.modelStats = {}

 
    def mapper(self, key, line):
        docID, docClass, text = line.split("\t",2)   
        words = text.split()
        for w in words:
            yield (docClass, (w, 1))
        yield (docClass, ("**Total", len(words))) # keep a tally of the total
        
    # TODO: Should add a combiner
    
    def reducer(self, classI, wordFreqs):
        #print "wordFreqs", wordFreqs
        #for w, freq in wordFreqs:   NOTE that this for loop exhausted the iterator; so can not iterate again later
        #      print "wordFreqs", classI, w, freq
        total = 0   #start of a new class
        currentWord = ""
        currentWordFreq = 0
        for w, freq  in wordFreqs:
            if (w == "**Total"): 
                total += freq
            elif currentWord == w:
                currentWordFreq += freq
            else: # a new word
                if currentWord != "**Total" and currentWord != "":
                    yield (classI, [currentWord, currentWordFreq, float(currentWordFreq)/total])
                currentWord = w
                currentWordFreq = freq
        #dont forget the last word!
        if currentWord !="" and currentWord != "**Total" :
            yield (classI, (currentWord, currentWordFreq, float(currentWordFreq)/total))

# The if __name__ == "__main__": 
# ... trick exists in Python so that our Python files 
# can act as either reusable modules, or as standalone programs.
if __name__ == '__main__':
    MrRelativeProbs.run()