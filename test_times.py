import os
import time
import subprocess
from subprocess import call


INPUT_PATH = "input/"
HADOOP_PATH = "kmeans-hadoop/"
SPARK_PATH = "kmeans-spark/kmeans.py"
SPARK_RESULT_PATH = "times/spark.txt"


for file in os.listdir(INPUT_PATH):
	variables = file.split(".")[0].split("_")
	d = variables[0][:-1]
	n = variables[1][:-1]
	k = variables[2][:-1]

	start_time = time.time()

	call(["spark-submit", SPARK_PATH, k, "0.5", file], stdout=subprocess.PIPE)

	end_time = time.time()

	duration = end_time - start_time

	with open(SPARK_RESULT_PATH,'w+') as spark_file:
		spark_file.write(str(d) + "," + str(n) + "," + str(k) + "," + str(duration) + "\n")



" hadoop jar kmeans-hadoop/target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeans k d n input output threshold"
