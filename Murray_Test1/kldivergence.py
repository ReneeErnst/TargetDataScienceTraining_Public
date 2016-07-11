
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import numpy as np


class kldivergence(MRJob):
    
    def mapper1(self, _, line):
        index = int(line.split('.',1)[0])
        letter_list = re.sub(r"[^A-Za-z]+", '', line).lower()
        count = {}
        for l in letter_list:
            if count.has_key(l):
                count[l] += 1
            else:
                count[l] = 1
        for key in count:
            yield (key, [index, count[key]*1.0/len(letter_list)])


    def reducer1(self, key, values):
        #Fill in your code
        p = 0
        q = 0
        for v in values:
            if v[0] == 1:  #String 1
                p += v[1]
            else:          # String 2
                q += v[1]

        yield (None, np.sum(np.where(p != 0 , p * np.log(p / q), 0)))

    def reducer2(self, key, values):
        kl_sum = 0
        for value in values:
            kl_sum = kl_sum + value
        yield None, kl_sum
            
    def steps(self):
        return [self.mr(mapper=self.mapper1,
                        reducer=self.reducer1),
                self.mr(reducer=self.reducer2)]

if __name__ == '__main__':
    kldivergence.run()