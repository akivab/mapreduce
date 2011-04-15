"""
Prefix sum.

For an array of values a_1, a_2, ... a_n, we want to
find s_1, s_2, ..., s_n s.t. s_i = sum_{j<=i}{a_j}

The solution requires lg(n) steps, and proceeds as
follows for each step:

Starting with s = 0
mapper:
for each a_i, yield <i; a_i> and <i + 2^s; a_i>

reducer:
sum, for each i, a_i + a_i-2^s

@author Akiva Bamberger
"""

from mrjob.job import MRJob
from math import log, ceil


class PrefixSum(MRJob):
    """
    Calculates the prefix-sum for an array of numbers
    """

    def __init__(self, *kwds, **args):
        super(PrefixSum,self).__init__(*kwds,**args)
        # we want k to range from 1..2^([lg(n)]+1)
        self.k = 1
    
    def mapper(self, i, a_i):
        if type(a_i) is not type(1):
            m = a_i.split()
            i = int(m[0])
            a_i = int(m[1])
            
        yield(i, -1)
        yield(i, a_i)
        yield(i+self.k, a_i)

    def reducer(self, i, nums):
        if -1 in nums:
            yield(i, sum(nums)+1)

if __name__ == '__main__':
    PrefixSum().run()
    

