from functools import reduce
"""
Reduce were used to hold the last operated output and perform with next iterator
"""

s = [1,2,3,4,5,6]
k = reduce(lambda x,y:x+y, s)
print(k)