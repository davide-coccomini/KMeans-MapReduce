import os


INPUT_PATH = "input/"
HADOOP_PATH = "kmeans-hadoop/"
SPARK_PATH = "kmeans-spark/kmeans.py"
SPARK_RESULT_FILE = "times/spark.txt"
HADOOP_RESULT_FILE = "times/hadoop.txt"

for file in os.listdir(INPUT_PATH):
	#print(file)
	variables = file.split(".")[0].split("_")
	d = variables[0][:-1]
	n = variables[1][:-1]
	k = variables[2][:-1]

	with open(SPARK_RESULT_FILE,'a') as spark_file:
		spark_file.write("spark-submit " + SPARK_PATH + " " + k + " 0.5 " + file +  "\n")

	with open(HADOOP_RESULT_FILE,'a') as hadoop_file:
	        hadoop_file.write("hadoop jar ../kmeans-hadoop/target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeans " + k + " " + d + " " + n + " " + file + " output" + file + " 0.5\n")

