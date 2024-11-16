"""
This file contains the node cluster class and the
individual node class. The cluster is a collection of workers/nodes
that will execute the map/reduce tasks. 
"""

import random

class NodeCluster:
    def __init__(self, num_nodes, tick_latency):
        self.num_nodes = num_nodes
        self.node_pool = []
        self.tick_latency = tick_latency

    def init_homogeneous_nodes(self):
        """
        This function should make homogeneous self.num_nodes number of nodes.
        Every node should have the same properties. 
        """
        # we need to have atleast 1 straggler
        number_of_straglers = random.randint(1, self.num_nodes / 2)
        # generate which node IDs will become stragglers
        straggler_list = [random.randint(0, self.num_nodes-1) for _ in range(number_of_straglers)]
        for i in range(0, self.num_nodes):
            rangeA = 1.5 # can adjust it later
            rangeB = 4 # can adjust it later
            if i in straggler_list:
                rangeA = 0.1 # can adjust it later
                rangeB = 1.5 # can adjust it later
            self.node_pool = Node(100, self.tick_latency, rangeA, rangeB) # setting it at 100 by default for now

    def init_heterogeneous_nodes(self, json_node_config):
        """
        This function would take a JSON node configuraion and would
        make heterogenous nodes. The number of nodes in the configuration
        should be equal to the self.num_nodes parameter.
        """
        pass

#TODO: add all the correct arguments to these node classes.
# As of now, I have just defined a basic skeleton structure. 
class Node:
    def __init__(self, total_tick, tick_latency, tick_rate_rangeA, tick_rate_rangeB):
        # initializing everything for now
        self.progress_score = 0
        self.time_to_completion = 0
        self.progress_rate = 0
        # this should be False be default - should be determined dynamically
        self.slow_status = False 
        self.total_tick = total_tick
        self.tick_rate = random.uniform(tick_rate_rangeA, tick_rate_rangeB)
        self.tick_latency = tick_latency

    def update_time_to_completion(self):
        self.time_to_completion = (1 - self.progress_score) / self.progress_rate

    def execute_map_task(self):
        pass

    def execute_reduce_task(self):
        pass

    def set_slow_status(self):
        self.slow_status = True
    
    def remove_slow_status(self):
        self.slow_status = False

