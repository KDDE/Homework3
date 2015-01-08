import random

fin = open('./lesson/movies.dat', 'r')
fout = open('userrating.dat', 'w')

movie_list = []
for line in fin:
    ll = line.strip().split('::')
    movie_list.append((ll[0], ll[1]))

random.shuffle(movie_list)

for i in range(10):
    rating = input(movie_list[i][1] + ' : ')
    fout.write('0::' + movie_list[i][0] + '::' + str(rating) + '\n')

fin.close()
fout.close()
