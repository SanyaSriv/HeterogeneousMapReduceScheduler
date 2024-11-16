"""
This file contains the node cluster class and the
individual node class. The cluster is a collection of workers/nodes
that will execute the map/reduce tasks. 
"""

class NodeCluster:
    def __init__(self, num_nodes):
        self.num_nodes = num_nodes

#TODO: add all the correct arguments to these node classes.
# As of now, I have just defined a basic skeleton structure. 
class Node:
    def __init__(self):
        pass

    def assign_map_task(self, mtask):
        pass

    def assign_reduce_task(self, rtask):
        pass

