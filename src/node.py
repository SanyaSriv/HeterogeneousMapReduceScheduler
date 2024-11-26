"""
This file contains the node cluster class and the
individual node class. The cluster is a collection of workers/nodes
that will execute the map/reduce tasks. 
"""

# TODO: According to this current implementation, the map task will execute in the same
    # time as the shufle task. Do we want to manipulate this? 
    # We can give more/less weightage to the other by manipulating the self.tick_rate parameter. 
    # We can make 3 such parameters - one for shuffle, other for map, and then another for reduce.

import random
import time
from log import InfoLogger

def form_log(msg):
    t = time.time()
    InfoLogger.info(f"LOGGING - NODE - [{t}] - {msg}")
class NodeCluster:
    def __init__(self, num_nodes, tick_latency, map_total_tick, reduce_total_tick, copy_total_tick, sort_total_tick):
        self.num_nodes = num_nodes # number of nodes (workers) to establish
        self.node_pool = {} # key: node ID, value: class instance
        self.tick_latency = tick_latency # amount of latency associated with every tick
        self.MAP_TOTAL_TICK = map_total_tick
        self.REDUCE_TOTAL_TICK = reduce_total_tick
        self.COPY_TOTAL_TICK = copy_total_tick
        self.SORT_TOTAL_TICK = sort_total_tick

    def set_scheduler(self, scheduler):
        self.sched = scheduler # can be late or hadoop (naive)

    def init_homogeneous_nodes(self, num_stragglers, lis_straggler, random_speed, deterministic_speed):
        """
        This function should make homogeneous self.num_nodes number of nodes.
        Every node should have the same properties. 
        """
        form_log(f"NODES CREATED")
        # we need to have atleast 1 straggler
        # number_of_straglers = random.randint(1, int(self.num_nodes / 2))
        number_of_straglers = num_stragglers
        # generate which node IDs will become stragglers
        # straggler_list = [random.randint(0, self.num_nodes-1) for _ in range(number_of_straglers)]
        straggler_list = lis_straggler
        for i in range(0, self.num_nodes): 
            if (random_speed == 1):
                # if this is turned on, then generate a random speed for the tasks   
                rangeA = 1.5 # can adjust it later
                rangeB = 4 # can adjust it later
                if i in straggler_list:
                    form_log(f"STRAGGLER: [NODE:{i}]")
                    rangeA = 0.1 # can adjust it later
                    rangeB = 1.5 # can adjust it later
                tick_rate = random.uniform(rangeA, rangeB)
            else:
                tick_rate = deterministic_speed[i]
            self.node_pool[i] = Node(i, 
                                    self.MAP_TOTAL_TICK, self.REDUCE_TOTAL_TICK, self.COPY_TOTAL_TICK, self.SORT_TOTAL_TICK,
                                    self.tick_latency, 
                                    tick_rate, 
                                    self.sched) # setting it at 100 by default for now
            form_log(f"CREATE-NODE: [NODE:{i}]")
    def set_slow_status(self, node_id):
        self.node_pool[node_id].mark_slow()

    def remove_slow_status(self, node_id):
        self.node_pool[node_id].remove_slow()

    # TODO: We are yet to implement this heterogenous configuration. We first aim to develop the
    # base LATE scheduler, and then add heterogeinity if time permits. 
    def init_heterogeneous_nodes(self, json_node_config):
        """
        This function would take a JSON node configuraion and would
        make heterogenous nodes. The number of nodes in the configuration
        should be equal to the self.num_nodes parameter.
        """
        pass

class Node:
    def __init__(self, node_id, map_total_tick, reduce_total_tick,
                copy_total_tick, sort_total_tick, tick_latency, tick_rate, sched):
        self.node_id = node_id
        self.slow_status = False 
        self.tick_rate = tick_rate
        self.tick_latency = tick_latency
        self.sched = sched
        self.MAP_TOTAL_TICK = map_total_tick
        self.REDUCE_TOTAL_TICK = reduce_total_tick
        self.COPY_TOTAL_TICK = copy_total_tick
        self.SORT_TOTAL_TICK = sort_total_tick

    def execute_map_task(self, task_id):
        # adding a redundant DUP parameter so we do not have to change the entire graph gen implementation
        form_log(f"BEGIN-MAP: [TASK:{task_id}] : [NODE:{self.node_id}] : [DUP:0]")
        temp_ticks = self.MAP_TOTAL_TICK
        while temp_ticks > 0:
            temp_ticks -= self.tick_rate

            current_tick_latency = self.tick_latency
            if self.node_id % 2 == 0 and self.sched.node_availibility[self.node_id + 1] == 0:
                current_tick_latency = 1.05 * current_tick_latency
            elif self.node_id % 2 == 1 and self.sched.node_availibility[self.node_id - 1] == 0:
                current_tick_latency = 1.05 * current_tick_latency
            time.sleep(current_tick_latency)
        
        # mark that the task if finished
        self.sched.mark_task_finished(task_id)
        # mark that the node is available
        self.sched.mark_node_available(self.node_id)
        form_log(f"DONE-MAP: [TASK:{task_id}] : [NODE:{self.node_id}] : [DUP:0]")

    def execute_red_task(self, task_id):
        form_log(f"BEGIN-COPY: [TASK:{task_id}] : [NODE:{self.node_id}] : [DUP:0]")
        temp_ticks = self.COPY_TOTAL_TICK
        while temp_ticks > 0:
            temp_ticks -= self.tick_rate
            current_tick_latency = self.tick_latency
            if self.node_id % 2 == 0 and self.sched.node_availibility[self.node_id + 1] == 0:
                current_tick_latency = 1.05 * current_tick_latency
            elif self.node_id % 2 == 1 and self.sched.node_availibility[self.node_id - 1] == 0:
                current_tick_latency = 1.05 * current_tick_latency
            time.sleep(self.tick_latency)
        form_log(f"DONE-COPY: [TASK:{task_id}] : [NODE:{self.node_id}] : [DUP:0]")

        # once copy is done, we can begin sort
        form_log(f"BEGIN-SORT: [TASK:{task_id}] : [NODE:{self.node_id}] : [DUP:0]")
        temp_ticks = self.SORT_TOTAL_TICK
        while temp_ticks > 0:
            temp_ticks -= self.tick_rate
            current_tick_latency = self.tick_latency
            if self.node_id % 2 == 0 and self.sched.node_availibility[self.node_id + 1] == 0:
                current_tick_latency = 1.05 * current_tick_latency
            elif self.node_id % 2 == 1 and self.sched.node_availibility[self.node_id - 1] == 0:
                current_tick_latency = 1.05 * current_tick_latency
            time.sleep(self.tick_latency)
        form_log(f"DONE-SORT: [TASK:{task_id}] : [NODE:{self.node_id}] : [DUP:0]")

        # once sort is done, we can begin reduce but we need to wait till all map tasks finish
        while (self.sched.map_tasks > 0):
            continue
        
        form_log(f"BEGIN-RED: [TASK:{task_id}] : [NODE:{self.node_id}] : [DUP:0]")
        temp_ticks = self.REDUCE_TOTAL_TICK
        while temp_ticks > 0:
            temp_ticks -= self.tick_rate
            current_tick_latency = self.tick_latency
            if self.node_id % 2 == 0 and self.sched.node_availibility[self.node_id + 1] == 0:
                current_tick_latency = 1.05 * current_tick_latency
            elif self.node_id % 2 == 1 and self.sched.node_availibility[self.node_id - 1] == 0:
                current_tick_latency = 1.05 * current_tick_latency
            time.sleep(self.tick_latency)
        self.sched.mark_task_finished(task_id)
        # mark that the node is available
        self.sched.mark_node_available(self.node_id)
        form_log(f"DONE-RED: [TASK:{task_id}] : [NODE:{self.node_id}] : [DUP:0]")

    def mark_slow(self):
        self.slow_status = True
    
    def remove_slow(self):
        self.slow_status = False