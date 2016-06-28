
# NOTE: I could set this to use one reducer as was asked, but there is no need to with the code below. 

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
 
from collections import defaultdict
 
from mrjob.job import MRJob
from mrjob.job import MRStep

import re, string

# Stores the number of emails by classifier
email_counts = dict()
 
# Stores the number of words by classifier
word_counts = dict()

class NaiveBayesEnron(MRJob):
    """
    A MRJob class for a Naive Bayes probability domain calculation of whether an email
    is SPAM or HAM (not-spam).
    """
 
    def __init__(self, *args, **kwargs):
        super(NaiveBayesEnron, self).__init__(*args, **kwargs)
     
    def configure_options(self):
        """
        Adding this allows switching between smoothing methods w/o having to copy all code 
        for each method and for changing the min length of words included in the model
        """
        super(NaiveBayesEnron, self).configure_options()
        self.add_passthrough_option(
            '--smoothing', default='none', choices=['none', 'laplace', 'jm'], 
            help='Specifies the type of smoothing to use'
        )
    
        self.add_passthrough_option(
            '--min-word-frequency', default=1, dest='min_word_frequency', type='int',
            help='Specifies the min length of a word to be included in model'
        )
        
        self.add_passthrough_option(
            '--lambda', default=0.3, dest='jm_lambda', type='float',
            help='Specifies the desired lambda value for JM smoothing'
        )
        
    def steps(self):
        out = [
            MRStep(
                mapper = self.mapper_one,
                combiner = self.combiner_one,
                reducer = self.reducer_one
            )
        ]
        
        if self.options.smoothing == 'laplace': 
            out.append(MRStep(
                reducer = self.reducer_laplace
            ))
        
        elif self.options.smoothing == 'jm':
            out.append(MRStep(
                reducer = self.reducer_jm
            ))
            
        else:
            out.append(MRStep(
                reducer = self.reducer_unsmoothed
            ))
        
        return out
 
    def mapper_one(self, _, email):
        """
        Read each email and map it to a collection of words and
        counts
 
        :param _:
            There is no key here because we're loading raw text
        :param email:
            An email to be processed
        :return:
            Yields a generator that returns (key, value) tuples for each unique
            word present in the email.
                * key: a tuple containing the word and the classifier for
                    the email
                * value: the number of times that word appeared in the email
        """
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        _, classifier, token = email.strip().split('\t', 2)
        token = regex.sub(' ', token.lower())
        token = re.sub( '\s+', ' ', token )
        
        words = token.split()
 
        # Counter for the number of emails processed by classifier
        yield (('EMAILS', classifier), 1)
 
        for word in set(words):                
            count = words.count(word)
 
            yield ((word, classifier), count)
 
            # Counter for the number of words processed by classifier
            yield (('WORDS', classifier), count)
 
 
    def combiner_one(self, word_classifier, counts):
        """
        Combine the raw output of each mapper so that the value for each key
        is now the number of times each word-classifier appeared in that
        mapper (not just in a particular email). This reduces the amount of
        data each mapper sends to the reducer.
 
        :param word_classifier:
            A tuple with a word and classifier combination
        :param counts:
            The number of counts that word + classifier combination was found
            in each email processed by a particular mapper.
        :return:
            Yields a generator that returns (key, value) tuples for each unique
            word processed by a particular mapper.
                * key: a tuple containing the word and the classifier for
                    the email
                * value: the number of times that word + classifier
                    combination appeared in the mapper
        """
 
        yield (word_classifier, sum(counts))
 
    def reducer_one(self, word_classifier, counts):
        """
        Collects the results of each mapper to get the total counts for each
        word + classifier combination within the processed document.
 
        :param word_classifier:
            A tuple with a word and classifier combination
        :param counts:
            The number of counts that word + classifier combination was found
            in each email processed by a particular mapper.
        :return:
            Yields a generator that returns (key, value) tuples for each unique
            word processed by a particular mapper.
                * key: a tuple containing the word and the classifier for
                    the email
                * value: the number of times that word + classifier
                    combination appeared in the mapper
        """
        # Calculate the count for each key 
        # e.g., for key (word, classifier) sum the list coming in from the combiner/mapper 
        # Could look like this: (the, 0):[1,4,1] will turn into (the, 0):6
        total_count = sum(counts)
        # split up the word_classifier tuple into word, classifier
        word, classifier = word_classifier
 
        # store the total number of words for a paticular classifier
        # By time the reducer is done, this will add the word count for each classifier to the word_counts dict
        # adding the return at the end of this it discards the "WORDS" keys 
        # as they are no longer needed after getting total_count
        if word == 'WORDS':
            if classifier not in word_counts:
                word_counts[classifier] = 0
                
            word_counts[classifier] += total_count
            return
        
        # store the total number of emails for a paticular classifier
        # By time the reducer is done, this will add the number of emails for each classifier to the email_counts dict 
        # Changed word from 'EMAILS' to 'PRIOR' because how we calculate priors is with the total num emails per classifier
        if word == 'EMAILS':
            email_counts[classifier] = total_count
            word = 'PRIOR'
 
        # This will be reduced again as we may get outputs for the same key more than once (if word appeared in both classifiers)
        # for example, (the, {0:3}) and (the, {1:5})
        # What the next reducer will receive in this case will look like this: (the, [{0:3}, {1:5}])
        if total_count < self.options.min_word_frequency:
            # Removes words from the total count if they were below the threshold and doesn't yield them to the
            # second stage reducers
            if classifier not in word_counts:
                word_counts[classifier] = 0
            word_counts[classifier] -= total_count
        else:
            yield (word, {classifier: total_count})
 
    def reducer_unsmoothed(self, word, classified_counts):
        """
        After the first reducer has completed, which populates the global
        variables needed to calculate probabilities from word counts, the
        results stream is processed to return the smoothed probabilities
 
        :param word:
            A unique word that was processed within the document
        :param classified_counts:
            A collection of dictionaries where the keys in each dictionary
            are the classifiers and the values are the number of times that
            word appeared within that classifier.
        :return:
            Prints the final Naive Bayes probabilities for later use
        """
 
        # dictionary where if the key doesn't exist, just give me 0
        combined = defaultdict(lambda: 0)
    
        # Creatse a single dictionary for a given word, with the key = classifier, and value = count
        # Using the example above this will result in {0:3, 1:5}
        for entry in classified_counts:
            for classifier, count in entry.items():
                combined[classifier] += count
 
        # for each classifier calculate the probability that the given word occured
        # if the word is 'PRIOR' it calculates the prior probability for that classifier
        for classifier in email_counts.keys():
            count = combined.get(classifier, 0)
 
            if word == 'PRIOR':
                probability = count / sum(email_counts.values())
            else:
                probability = count / word_counts[classifier]
 
            yield (word, classifier), probability
    
    def reducer_laplace(self, word, classified_counts):
        """
        This is the laplace smoothed version of the second stage reducer (reducer_unsmoothed)
        """
        
        combined = defaultdict(lambda: 0)
        
        for entry in classified_counts:
            for classifier, count in entry.items():
                combined[classifier] += count
 
        for classifier in email_counts.keys():
            count = combined.get(classifier, 0)
 
            if word == 'PRIOR':
                probability = count / sum(email_counts.values())
            else:
                probability = (count + 1) / (word_counts[classifier] + 2)
 
            yield (word, classifier), probability
    
    def reducer_jm(self, word, classified_counts):
        """
        This is the Jelinek-Mercer smoothed version of the second stage reducer (reducer_unsmoothed)
        """
        
        combined = defaultdict(lambda: 0)
        
        for entry in classified_counts:
            for classifier, count in entry.items():
                combined[classifier] += count
 
        for classifier in email_counts.keys():
            count = combined.get(classifier, 0)

        for classifier in email_counts.keys():
            count = combined.get(classifier, 0)
            jm_lambda = self.options.jm_lambda
        
            if word == 'PRIOR':
                probability = count / sum(email_counts.values())
            else:
                total_this_word = sum(combined.values())
                total_all_words = sum(word_counts.values())
                
                probability = (
                    (1 - jm_lambda) * (count / word_counts[classifier]) +
                    (jm_lambda * total_this_word / total_all_words)
                )
                
            yield (word, classifier), probability 

if __name__ == '__main__':
    NaiveBayesEnron.run()