
"""An implementation of wc as an MRJob.
This is meant as an example of why mapper_final is useful."""
from mrjob.job import MRJob

class MRNaiveBayesTrainer(MRJob):

    def __init__(self, *args, **kwargs):
        super(MRNaiveBayesTrainer, self).__init__(*args, **kwargs)
        self.modelStats = {}

    def mapper(self, _, line):
        # Don't actually yield anything for each line. Instead, collect them
        # and yield the sums when all lines have been processed. The results
        # will be collected by the reducer.
        docID, docClass,text = line.split("\t",2)   
        words = text.split()
        if docID != "D5":  #skip doc d5 in chinese dataset
            if docClass == "1":
                yield("TomsPriors", "0,1")
                for word in words:
                    yield(word, "0,1")
            else:
                yield("TomsPriors", "1,0")
                for word in words:
                    yield(word, "1,0")
        

    def reducer(self, word, values):
        #aggregate counts for Pr(Word|Class)
        #yield("number of values for "+word, str(values))
        w0Total=0
        w1Total=0
        for value in values:
            w0, w1 =  value.split(",")
            w0Total += float(w0)
            w1Total += float(w1)  
        self.modelStats[word] =  [w0Total, w1Total]

        #yield("JIMI "+word, [w0Total, w1Total])
    def reducer_final(self):
        
        class0Total = 0
        class1Total = 0
        for k in self.modelStats.keys():
            if k != "TomsPriors":
                class0Total += self.modelStats[k][0]
                class1Total += self.modelStats[k][1]
        vocabularySize = len(self.modelStats.keys()) -1  #ignore TomsPriors
        yield ("defaultPrior 0 class", class0Total+vocabularySize)
        yield ("defaultPrior 1 class", class1Total+vocabularySize)
        yield ("count 0 class", class0Total)
        yield ("count 1 class", class1Total)
        yield ("vocabularySize", vocabularySize)
        #calculate priors 
        classCount0, classCount1 = self.modelStats.get("TomsPriors")
        del self.modelStats["TomsPriors"]
        total = classCount0 + classCount1
        yield("TomsPriors", ','.join(str(j) for j in [classCount0, classCount1, classCount0/total, classCount1/total])) 
        for k in self.modelStats.keys():
            yield(k, ','.join(str(j) for j in [self.modelStats[k][0],
                      self.modelStats[k][1],
                      (self.modelStats[k][0] + 1) /(class0Total + vocabularySize), 
                      (self.modelStats[k][1] +1)/(class1Total+vocabularySize)]))        

if __name__ == '__main__':
    MRNaiveBayesTrainer.run()