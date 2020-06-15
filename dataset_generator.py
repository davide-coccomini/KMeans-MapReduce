import sys
from sklearn.datasets import make_blobs

DATA_PATH = "input/"


if len(sys.argv) < 4:
	print("Usage: dataset_generator <d1[,d2,..,dn]> <n1[,n2,..,nn]> <k1[,k2,..,kn]>", file=sys.stdout)
	sys.exit(-1)

d_list = sys.argv[1].split(',')
n_list = sys.argv[2].split(',')
k_list = sys.argv[3].split(',')

for d in d_list:
	for n in n_list:
		for k in k_list:
			filename = DATA_PATH + d + 'd_' + n + 'n_' + k + 'k.txt'

			d_int = int(d)
			n_int = int(n)
			k_int = int(k)

			X, y = make_blobs(n_samples=n_int, n_features=d_int, centers=k_int)

			with open(filename, 'a') as file:
				for index, point in enumerate(X):
					file.write(','.join(map(str, point)))
					if(index < len(X) - 1):
						file.write('\n')
