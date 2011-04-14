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

        for u,v in E:
            # if both vertices have been seen before
            if V.get(u) and V.get(v):
                # see if both are the same component
                if related(V,v,u):
                    continue
                
                # if not, append this edge to list
                else:
                    yield(i,(u,v))
                    join(V,v,u)

            elif not V.get(u) or not V.get(v):

                yield(i,(u,v))
                            
                if not V.get(u) and not V.get(v):
                    # if both were unseen, add
                    # them to the list of vertices
                    V[u] = u
                    V[v] = u

                elif not V.get(u):
                    V[u] = ancestor(V,v)
                else:
                    V[v] = ancestor(V,u)

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
        total = 0
        for u,v in F:
            if V.get(u) and V.get(v):
                if related(V,v,u):
                    continue
                else:
                    total = total - 1
                    join(V,v,u)

            else:
                if not V.get(u) and not V.get(v):
                    V[u] = u
                    V[v] = u
                    total = total + 1
                elif not V.get(u):
                    V[u] = ancestor(V,v)
                else:
                    V[v] = ancestor(V,u)
                
        yield("$", total)

    def steps(self):
        return ([self.mr(mapper=self.mapper1,reducer=self.reducer1),
                 self.mr(mapper=self.mapper2,reducer=self.reducer2)])

def related(V, v, u):
    """
    Lets us know if v, u are in same component or not
    """
    return ancestor(V,v) == ancestor(V,u)
    

def ancestor(V, u):
    """
    Returns the ancestor of u in V
    """
    u_ = u
    while V[u_] is not u_: u_ = V[u_]
    return u_

def join(V,v,u):
    v_ = v
    u_ = u
    r = ancestor(V,u)
    while V[v_] is not v_: 
        t = V[v_]
        V[v_] = r
        v_ = t
    V[v_] = r
    
    while V[u_] is not u_:
        t = V[u_]
        V[u_] = r
        u_ = t

if __name__ == '__main__':
    FindComponents.run()


