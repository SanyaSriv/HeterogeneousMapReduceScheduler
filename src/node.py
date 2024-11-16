"""
This file contains the node cluster class and the
individual node class. The cluster is a collection of workers/nodes
that will execute the map/reduce tasks. 
"""

class NodeCluster:
    def __init__(self, num_nodes):
        self.num_nodes = num_nodes

    def init_homogeneous_nodes():
        """
        This function should make homogeneous self.num_nodes number of nodes.
        Every node should have the same properties. 
        """
        pass

    def init_heterogeneous_nodes(json_node_config):
        """
        This function would take a JSON node configuraion and would
        make heterogenous nodes. The number of nodes in the configuration
        should be equal to the self.num_nodes parameter.
        """
        pass

#TODO: add all the correct arguments to these node classes.
# As of now, I have just defined a basic skeleton structure. 
class Node:
    def __init__(self):
        # initializing everything for now
        self.progress_score = 0
        self.time_to_completion = 0
        self.progress_rate = 0
        self.slow_status = False # this should be False be default

    def update_time_to_completion(self):
        self.time_to_completion = (1 - self.progress_score) / self.progress_rate

    def execute_map_task(self, mtask):
        pass

    def execute_reduce_task(self, rtask):
        pass

    def set_slow_status(self):
        self.slow_status = True
    
    def remove_slow_status(self):
        self.slow_status = False

