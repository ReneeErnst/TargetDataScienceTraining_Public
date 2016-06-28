from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from mrjob.job import MRJob
from mrjob.step import MRStep
import os, re, string, math

counts = []
email_posteriors = dict()

class NaiveBayesEnronClassifier(MRJob):


    def __init__(self, *args, **kwargs):
        super(NaiveBayesEnronClassifier, self).__init__(*args, **kwargs)
        
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init, 
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer  
            ),
            MRStep(
                reducer=self.reducer_two
            )
        ]

    def configure_options(self):
        super(NaiveBayesEnronClassifier, self).configure_options()
        
        self.add_file_option('--model')
        
    def mapper_init(self): 
        self.model_stats = {}

        with open(self.options.model, "r") as f:
            lines = f.read().split('\n')
        
        split_lines = [line.split('\t') for line in lines]
        del lines # Deleting the the line variable to save memory
        
        for entry in split_lines:
            word = entry[0]
            probabilities = [float(p) for p in entry[1:]]
            self.model_stats[word] = probabilities
    
    def mapper(self, _, line):
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        _, classifier, token = line.strip().split('\t', 2)
        token = regex.sub(' ', token.lower())
        token = re.sub( '\s+', ' ', token )
        
        # set initial values to the priors obtainined in model_stats
        posteriors = [math.log10(prior) for prior in self.model_stats['PRIOR']]
        
        for word in token.split():
            # Get the value for the word, or if it is not in model_stats, return 0 as the default
            probabilities = self.model_stats.get(word, [0, 0]) 
            
            # converts all 0s to 1s - note that log10 of 1 = 0
            probabilities = [p if p > 0 else 1 for p in probabilities] 
           
            for index, probability in enumerate(probabilities):
                if probability == 1:
                    yield('zero_count_{}'.format(index), 1)
        
            posteriors = [x + math.log10(y) for x, y in zip(posteriors, probabilities)]
            
        for index, posterior in enumerate(posteriors):
            yield('post_{}'.format(index), posterior)
            
        # Determine which class the email belongs to (SPAM vs HAM)
        # If p_class_0 = p_class_1, class is unknown
        max_posterior = max(posteriors)
        max_counts = [1 if p == max_posterior else 0 for p in posteriors]
        if sum(max_counts) > 1:
            predicted_class = -1
        else:
            predicted_class = posteriors.index(max_posterior)
            
        # Determine if our predicted class is correct
        if predicted_class == int(classifier):
            key = 'correct'
        else:
            key = 'incorrect'
            
        yield (key, 1)
        yield ('email_count', 1)

    def combiner(self, key, values):
        
        if key.startswith('post_'):
            # Keep a list of the posteriors for each class type for histogram
            key = key.split('_')[-1]
            if key not in email_posteriors:
                email_posteriors[key] = []
            email_posteriors[key] += list(values)
            return
        
        yield (key, sum(values))
        
    def reducer(self, key, values):
        
        if key.startswith('post_'):
            # Keep a list of the posteriors for each class type for histogram
            key = key.split('_')[-1]
            if key not in email_posteriors:
                email_posteriors[key] = []
            email_posteriors[key] += list(values)
            return
        
        count = sum(values)
        
        if key in ['correct', 'incorrect']:
            counts.append(count)
            
        yield (key, count)
      
    def reducer_two(self, key, values):
        values = list(values)
        
        if key in ['correct', 'incorrect']:
            rate = sum(values) / sum(counts)
            out_key = 'error_rate' if key == 'incorrect' else 'accuracy'
            yield (out_key, rate)
        else:
            yield (key, sum(values))

if __name__ == '__main__':
    NaiveBayesEnronClassifier.run()