from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
from numpy import argmin
from numpy import array


def minimum_distance(data_point, centroid_points):
    # Calculate find the nearest centroid for data point

    data_point = array(data_point)
    centroid_points = array(centroid_points)
    diff = data_point - centroid_points
    diff_squared = diff * diff

    # Get the nearest centroid for each instance
    min_index = argmin(list(diff_squared.sum(axis=1)))
    return min_index


class MRKmeansIteration(MRJob):
    k = 3

    def __init__(self, *args, **kwargs):
        super(MRKmeansIteration, self).__init__(*args, **kwargs)
        self.centroid_points = []

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]

    def configure_options(self):
        """Add command-line options specific to this script."""
        super(MRKmeansIteration, self).configure_options()

        self.add_passthrough_option(
            '--centroidsFile',
            dest='centroidsFile',
            default="Centroids.txt",
            type='str'
        )

    def mapper_init(self):
        """load centroids info from file"""

        # print("Current path:", os.path.dirname(os.path.realpath(__file__)))

        with open(self.options.centroidsFile, 'r') as f:
            centroids = f.read().strip().split('\n')

        self.centroid_points = [
            [float(v) for v in s.split(',')] for s in centroids
        ]

        # print("Centroids: ", self.centroid_points)

    def mapper(self, _, line):
        # load data and output the nearest centroid index and data point

        point = [float(v) for v in line.split(',')]
        index = int(minimum_distance(point, self.centroid_points))
        weight = 1.0 / np.sqrt(point[0] ** 2 + point[1] ** 2)

        yield (
            index,
            (weight * point[0], weight * point[1], weight, 1)
        )

    def combiner(self, index, input_data):
        # Combine sum of data points locally

        x_sum = 0
        y_sum = 0
        count = 0
        weight_sum = 0

        for x, y, weight, n in input_data:
            count += n
            x_sum += x
            y_sum += y
            weight_sum += weight

        yield index, (x_sum, y_sum, weight_sum, count)

    def reducer(self, index, input_data):
        # Aggregate sum for each cluster and then calculate the new centroids

        x_sum = 0
        y_sum = 0
        count = 0
        weight_sum = 0

        for x, y, weight, n in input_data:
            count += n
            x_sum += x
            y_sum += y
            weight_sum += weight

        # Calculate the centroid as the weighted mean
        x_mean = x_sum / weight_sum
        y_mean = y_sum / weight_sum

        # send the centroids back to the driver
        yield index, (x_mean, y_mean)


if __name__ == '__main__':
    MRKmeansIteration.run()