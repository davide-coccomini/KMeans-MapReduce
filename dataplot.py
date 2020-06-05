import os
from os.path import isfile
import json
import numpy
import matplotlib.pyplot as mplot
from mpl_toolkits import mplot3d


DATA_PATH = "input/"
RESULT_PATH = "result/"


def iris():
	iris_dataset_file = open(DATA_PATH + "iris.txt", "r")
	iris_dataset_data = numpy.loadtxt(iris_dataset_file, delimiter=",")

	print(iris_dataset_data.shape)

	sepal_lenght_data = iris_dataset_data[:, 0]
	sepal_width_data = iris_dataset_data[:, 1]
	petal_lenght_data = iris_dataset_data[:, 2]
	petal_width_data = iris_dataset_data[:, 3]

	centroids = [
		[5.720886075949368, 2.9974683544303784, 3.613924050632914, 1.1246835443037964],
		[6.80238095238095, 3.05, 5.669047619047619, 2.045238095238095],
		[5.006, 3.4179999999999997, 1.464, 0.24399999999999988]
	]

	centroids = numpy.asarray(centroids)

	centroid_sepal_lenght_data = centroids[:, 0]
	centroid_sepal_width_data = centroids[:, 1]
	centroid_petal_lenght_data = centroids[:, 2]
	centroid_petal_width_data = centroids[:, 3]

	fig = mplot.figure()
	fig.suptitle("Iris", fontsize=16)

	ax = fig.add_subplot(2, 3, 1, autoscale_on=True)
	ax.set_xlabel("sepal_lenght")
	ax.set_ylabel("sepal_width")
	ax.scatter(sepal_lenght_data, sepal_width_data, alpha=0.5, c='blue')
	ax.scatter(centroid_sepal_lenght_data, centroid_sepal_width_data, alpha=0.5, c='red')

	ax = fig.add_subplot(2, 3, 2, autoscale_on=True)
	ax.set_xlabel("sepal_lenght")
	ax.set_ylabel("petal_lenght")
	ax.scatter(sepal_lenght_data, petal_lenght_data, alpha=0.5, c='blue')
	ax.scatter(centroid_sepal_lenght_data, centroid_petal_lenght_data, alpha=0.5, c='red')

	ax = fig.add_subplot(2, 3, 3, autoscale_on=True)
	ax.set_xlabel("sepal_lenght")
	ax.set_ylabel("petal_width")
	ax.scatter(sepal_lenght_data, petal_width_data, alpha=0.5, c='blue')
	ax.scatter(centroid_sepal_lenght_data, centroid_petal_width_data, alpha=0.5, c='red')

	ax = fig.add_subplot(2, 3, 4, autoscale_on=True)
	ax.set_xlabel("sepal_width")
	ax.set_ylabel("petal_lenght")
	ax.scatter(sepal_width_data, petal_lenght_data, alpha=0.5, c='blue')
	ax.scatter(centroid_sepal_width_data, centroid_petal_lenght_data, alpha=0.5, c='red')

	ax = fig.add_subplot(2, 3, 5, autoscale_on=True)
	ax.set_xlabel("sepal_width")
	ax.set_ylabel("petal_width")
	ax.scatter(sepal_width_data, petal_width_data, alpha=0.5, c='blue')
	ax.scatter(centroid_sepal_width_data, centroid_petal_width_data, alpha=0.5, c='red')

	ax = fig.add_subplot(2, 3, 6, autoscale_on=True)
	ax.set_xlabel("petal_lenght")
	ax.set_ylabel("petal_width")
	ax.scatter(petal_lenght_data, petal_width_data, alpha=0.5, c='blue')
	ax.scatter(centroid_petal_lenght_data, centroid_petal_width_data, alpha=0.5, c='red')

	mplot.show()


def test():
	iris_dataset_file = open(DATA_PATH + "3d1000n.txt", "r")
	iris_dataset_data = numpy.loadtxt(iris_dataset_file, delimiter=",")

	print(iris_dataset_data.shape)

	sepal_lenght_data = iris_dataset_data[:, 0]
	sepal_width_data = iris_dataset_data[:, 1]

	centroids = [
		[40.62903225806452, 52.70967741935484],
		[48.17391304347826, 24.956521739130434],
		[42.0, 9.2]
	]

	centroids = numpy.asarray(centroids)

	centroid_sepal_lenght_data = centroids[:, 0]
	centroid_sepal_width_data = centroids[:, 1]

	fig = mplot.figure()
	fig.suptitle("Test", fontsize=16)

	ax = fig.add_subplot(1, 1, 1, autoscale_on=True)
	ax.scatter(sepal_lenght_data, sepal_width_data, alpha=0.5, c='blue')
	ax.scatter(centroid_sepal_lenght_data, centroid_sepal_width_data, alpha=0.5, c='red')

	mplot.show()


test()
