
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import sys, time, json
#sys.path.append('/usr/lib/python2.4/site-packages/')
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from pprint import pprint

class ProcessJSONRecords(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json_value'
    DEFAULT_OUTPUT_PROTOCOL = 'repr_value'

    def mapper(self, _, lineStr):
        line = json.loads(lineStr)
        print("full JSON Dictionary", line)
        emailID = line['email']['id']
        print("emailID is ", emailID)
        label = line['email']['Label']
        content = line["email"]["content"]
        print(line["email"]["Label"], line["email"]["content"])
        yield line["email"]["Label"], line["email"]["content"]

    def reducer(self, label, emailBodies):
        #for bodyText in emailBodies:
            #line_data='\t'.join(str(n) for n in d)
        #    yield label, str(bodyText)
        print(label+"Reducer", emailBodies)


if __name__ == '__main__':
    ProcessJSONRecords.run()