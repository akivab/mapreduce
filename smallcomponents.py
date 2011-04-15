"""
Attempts to find components using o(|V|) space.

@author Akiva Bamberger
"""
from mrjob.job import MRJob

class FindComponents(MRJob):
    count = 0
    def mapper(self, _, v):
        if type(v) == type(""):
            v = [int(i) for i in v.split()]
            yield (v[0], [v[1]])
            yield (v[1], [v[0]])
        else:
            for i in v:
                yield (i, v)

    def reducer(self, u, v):
        N = [u]
        for i in v:
            N.extend(i)
        N = sorted(list(set(N)))
        yield (u, N)

    def mapper1(self, i, v):
        yield (str(v), i)

    def reducer1(self, u, v):
        self.count = self.count+1
        yield (self.count, u)

    def steps(self):
        a = [self.mr(mapper=self.mapper, reducer=self.reducer)] * 10
        a.append(self.mr(mapper=self.mapper1, reducer=self.reducer1))
        return a

if __name__ == '__main__':
    FindComponents.run()
