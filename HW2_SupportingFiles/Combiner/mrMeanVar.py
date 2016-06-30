from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from math import sqrt   
import json

# given 4,5,15,3,20, 3,6,4000
# Mean = 507, SD = 1411
# Calculate the mean and standard deviation
#
class mrMeanVar(MRJob):
    DEFAULT_PROTOCOL = 'json'  #split records in key value pairs using TAB

    def mapper(self, key, line):
        lineDict = json.loads(line)
        val = lineDict["value"]
        yield (1,(val, val*val))
        
    

    def reducer(self, key, vals):
        N = 0.0
        sum = 0.0
        sumsq = 0.0
        for val, valSqd in vals:
            N += 1
            sum += val
            sumsq += valSqd
        mean = sum / N
        sd = sqrt((sumsq - sum*sum / N) / (N - 1))
        results = [mean, sd]
        yield (1, results)

if __name__ == '__main__':
    mrMeanVar.run()