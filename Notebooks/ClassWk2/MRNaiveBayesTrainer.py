
from mrjob.job import MRJob

class MRNaiveBayesTrainer(MRJob):

    def __init__(self, *args, **kwargs):
        super(MRNaiveBayesTrainer, self).__init__(*args, **kwargs)
        self.modelStats = {}
        self.vocab = {}
        self.count = 0
        classTotalFreq = [0, 0]

    def jobconf(self):
        orig_jobconf = super(MRNaiveBayesTrainer, self).jobconf()        
        custom_jobconf = {
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k1rn',
            'mapred.reduce.tasks': '1',
        }
        combined_jobconf = orig_jobconf
        combined_jobconf.update(custom_jobconf)
        self.jobconf = combined_jobconf
        return combined_jobconf

    def mapper(self, _, line):
        # Don't actually yield anything for each line. Instead, collect them
        # and yield the sums when all lines have been processed. The results
        # will be collected by the reducer.
        docID, docClass, text = line.split("\t", 2)   
        words = text.split()
        for word in words:
            self.vocab[word] = 1
        if docID != "D5":  #skip doc d5 in chinese dataset
            if docClass == "1":
                yield ("TomsPriors", "0,1")
                yield ("*classTotalFreq", "0," + str(len(words)))
                for word in words:
                    yield (word, "0,1")
            else:
                yield ("TomsPriors", "1,0")
                yield ("*classTotalFreq", str(len(words)) + ",0")
                for word in words:
                    yield (word, "1,0")
        for key in self.vocab.keys():
            yield ("*!" + key, "1,1")

    def reducer_ck(self, word, values):
        for value in values:
            yield (word, value)

    def reducer(self, word, values):
        #aggregate counts for Pr(Word|Class)
        #yield("number of values for "+word, str(values))
        w0Total = 0
        w1Total = 0
        for value in values:
                w0, w1 =  value.split(",")
                w0Total += float(w0)
                w1Total += float(w1)                    #this is what we added in class to go stateless...
        if word == "*classTotalFreq":
            self.modelStats[word] = [w0Total, w1Total]
        elif word.startswith("*!"):
            self.count += 1
        elif word == "TomsPriors":
            yield ("TomsPriors", str(w0Total / (w0Total + w1Total))+ "," + str(w1Total / (w0Total + w1Total)))
        else: 
            yield (word, str(w0Total / self.modelStats["*classTotalFreq"][0]) + 
                  "," + str(w1Total / self.modelStats["*classTotalFreq"][1]))


if __name__ == '__main__':
    MRNaiveBayesTrainer.run()