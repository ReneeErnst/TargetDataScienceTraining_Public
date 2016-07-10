from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob


class ComplaintTypeCount(MRJob):
    
    
    def mapper(self, _, line):
        # converting bytes to utf8 string - deals non-basic/keyboard characters
        line = line.decode('utf8', 'ignore')

        complaint_id, complaint_type, _ = line.split(',', 2)
        complaint_type = complaint_type.lower().strip().replace(' ', '_')
        del _

        # If it can't convert complaint id to an int, 
        # then skip that row of data (i.e., skip the header in the csv)
        try:
            int(complaint_id)
        except: 
            return

        if complaint_type not in ['debt_collection', 'mortgage']:
            complaint_type = 'other'
        self.increment_counter('complaint_type', complaint_type, 1)
        yield (complaint_type, 1)
        
    def combiner(self, complaint, counts):
        yield (complaint, sum(counts))
        
    def reducer(self, complaint, counts):
        yield (complaint, sum(counts))


if __name__ == '__main__':
    ComplaintTypeCount.run()