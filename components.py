"""
Finds components in a graph using MRJob,
a library built by Yelp for making MapReduce
manageable.

@author Akiva Bamberger
"""
from mrjob.job import MRJob
from random import randint

class FindComponents(MRJob):
    """
    Finds components in a graph
    """
    def mapper1(self, _, line):
        """
        We use k to determine the number of subgraphs
        to break the original graph into.
        """
        k = 10
        V = [int(i) for i in line.split()]
        r = randint(1,k)
        yield (r, (V[0],V[1])) 

    def reducer1(self, i, E):
        """
        Reducer to remove any unnecessary edges
        """
        V = {}
        F = []
        comp = 1
        check = {}

        for u,v in E:
            # if both vertices have been seen before
            if V.get(u) and V.get(v):
                # see if both are the same component
                try:
                    check[V[u]].index(V[v])
                    continue
                
                # if not, append this edge to list
                except ValueError:
                    yield (i, (u, v))
                    # V[u] != V[v] before, but
                    # now V[v] == V[u] (same comp)
                    check[V[u]].append(V[v])
                    check[V[v]].append(V[u])

            elif not V.get(u) or not V.get(v):
                # some vertex must have been unseen
                # if we got here. add this edge
                # then -- it introduces a new
                # vertex to a component.
                yield (i, (u, v))
                
                if not V.get(u) and not V.get(v):
                    # if both were unseen, add
                    # them to the list of vertices
                    V[u] = comp
                    V[v] = comp
                    check[comp] = [comp]
                    comp = comp + 1

                elif not V.get(u):
                    V[u] = V[v]
                else:
                    V[v] = V[u]

    def mapper2(self, _, E):
        """
        For each edge that comes in, we return that edge
        with the key "$" because this method is MONEY.
        """
        yield("$", E)

    def reducer2(self, _, F):
        """
        Similar to reducer1.
        """
        V = {}
        comp = 1
        total = 0
        check = {}
        for u,v in F:
            if V.get(u) and V.get(v):
                try:
                    check[V[u]].index(V[v])
                    continue
                except ValueError:
                    total = total - 1
                    check[V[u]].append(V[v])
                    check[V[v]].append(V[u])
            else:
                if not V.get(u) and not V.get(v):
                    # if both were unseen, add
                    # them to the list of vertices
                    V[u] = comp
                    V[v] = comp
                    check[comp] = [comp]
                    comp = comp + 1
                    total = total + 1
                elif not V.get(u):
                    V[u] = V[v]
                else:
                    V[v] = V[u]
                
        yield("$", total)

    def steps(self):
        return ([self.mr(mapper=self.mapper1,reducer=self.reducer1),
                 self.mr(mapper=self.mapper2,reducer=self.reducer2)])

if __name__ == '__main__':
    FindComponents.run()


