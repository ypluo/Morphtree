import sys

fin = open(sys.argv[1], 'r')
fout = open(sys.argv[1].strip(".txt") + "_ext.txt", 'w')

res = [[], [], [], [], [], [], [], [], [], []]
num = 0

line = fin.readline()
while line != "":
	if "index type" in line:
		num += 1
	else :
		res[num - 1].append(float(line))
	line = fin.readline()
fin.close()

max_len = max([len(x) for x in res])
for i in range(max_len):
	s = ''
	for j in range(num):
		s += str(res[j][i]) if i < len(res[j]) else "0.00"
		s += " "
	fout.write(s + '\n')