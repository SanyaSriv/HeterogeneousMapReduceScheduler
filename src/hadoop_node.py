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
import threading

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

    def init_homogeneous_nodes(self):
        """
        This function should make homogeneous self.num_nodes number of nodes.
        Every node should have the same properties. 
        """
        # we need to have atleast 1 straggler
        number_of_straglers = random.randint(1, int(self.num_nodes / 2))
        # generate which node IDs will become stragglers
        straggler_list = [random.randint(0, self.num_nodes-1) for _ in range(number_of_straglers)]
        for i in range(0, self.num_nodes):
            rangeA = 1.5 # can adjust it later
            rangeB = 4 # can adjust it later
            if i in straggler_list:
                rangeA = 0.1 # can adjust it later
                rangeB = 1.5 # can adjust it later
            self.node_pool[i] = Node(i, 
                                    self.MAP_TOTAL_TICK, self.REDUCE_TOTAL_TICK, self.COPY_TOTAL_TICK, self.SORT_TOTAL_TICK,
                                    self.tick_latency, 
                                    rangeA, rangeB, 
                                    self.sched) # setting it at 100 by default for now

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
                copy_total_tick, sort_total_tick, tick_latency, tick_rate_rangeA, tick_rate_rangeB, sched):
        self.node_id = node_id
        self.slow_status = False 
        self.tick_rate = random.uniform(tick_rate_rangeA, tick_rate_rangeB)
        self.tick_latency = tick_latency
        self.sched = sched
        self.MAP_TOTAL_TICK = map_total_tick
        self.REDUCE_TOTAL_TICK = reduce_total_tick
        self.COPY_TOTAL_TICK = copy_total_tick
        self.SORT_TOTAL_TICK = sort_total_tick

    def execute_map_task(self, task_id, dup):
        t = 0
        temp_ticks = self.MAP_TOTAL_TICK
        while temp_ticks > 0:
            if task_id in self.sched.task_completion_flag:
                with self.sched.lock_nodes:
                    self.sched.available_nodes.append(self.node_id)
                    print("Redundant Map task aborted",task_id, self.node_id, self.sched.available_nodes)
                    return
            temp_ticks -= self.tick_rate
            time.sleep(self.tick_latency * 1.01 ** (len(self.sched.duplicate_tasks)+1))
            t += self.tick_latency
            if self.sched.id in ["late", "hadoop"]:
                ret = self.sched.update_node_progress(self.node_id,(1 - temp_ticks/self.MAP_TOTAL_TICK), t, task_id, dup)
        # once it is done, it should add a copy task to the list of tasks
        if dup:
            self.sched.duplicate_tasks[task_id][1] = 1
        else:
            self.sched.running_tasks[task_id][1] = 1
        self.sched.task_completion_flag[task_id] = True
        with self.sched.lock_nodes:
            self.sched.available_nodes.append(self.node_id)
            print("Map done", task_id, self.node_id, dup, self.sched.available_nodes)
    
    def execute_reduce_task(self, task_id, dup):
        t=0
        temp_ticks = self.COPY_TOTAL_TICK
        while temp_ticks > 0:
            if task_id in self.sched.task_completion_flag:
                with self.sched.lock_nodes:
                    self.sched.available_nodes.append(self.node_id)
                    print("Redundant reduce task aborted in copy phase")
                    return
            temp_ticks -= self.tick_rate
            time.sleep(self.tick_latency * 1.01 ** (len(self.sched.duplicate_tasks)+1))
            t += self.tick_latency
            if self.sched.id in ["late", "hadoop"]:
                ret = self.sched.update_node_progress(self.node_id, (1-temp_ticks/self.COPY_TOTAL_TICK)/3, t, task_id, dup)

        print("Copy done", task_id, self.sched.available_nodes)

        temp_ticks = self.SORT_TOTAL_TICK
        while temp_ticks > 0:
            if task_id in self.sched.task_completion_flag:
                with self.sched.lock_nodes:
                    self.sched.available_nodes.append(self.node_id)
                    print("Redundant reduce task aborted in sort phase")
                    return
            temp_ticks -= self.tick_rate
            time.sleep(self.tick_latency)
            t += self.tick_latency
            if self.sched.id in ["late", "hadoop"]:
                ret = self.sched.update_node_progress(self.node_id, 1/3 + (1-temp_ticks/self.SORT_TOTAL_TICK)/3, t, task_id, dup)
        print("Sort done", task_id, self.sched.available_nodes)

        while True:
            if self.sched.num_completion == self.sched.map_num:
                break

        temp_ticks = self.REDUCE_TOTAL_TICK
        while temp_ticks > 0:
            if task_id in self.sched.task_completion_flag:
                with self.sched.lock_nodes:
                    self.sched.available_nodes.append(self.node_id)
                    print("Redundant reduce task aborted")
                    return
            temp_ticks -= self.tick_rate
            time.sleep(self.tick_latency)
            t += self.tick_latency
            if self.sched.id in ["late", "hadoop"]:
                ret = self.sched.update_node_progress(self.node_id, 2/3 + (1-temp_ticks/self.REDUCE_TOTAL_TICK)/3, t, task_id, dup)
        if task_id not in self.sched.task_completion_flag:
            if dup:
                self.sched.duplicate_tasks[task_id][1] = 1
            else:
                self.sched.running_tasks[task_id][1] = 1
        self.sched.task_completion_flag[task_id] = True
        with self.sched.lock_nodes:
            self.sched.available_nodes.append(self.node_id)
        # once it is done, it would not add any more tasks
        print("Red Done", task_id, self.node_id, self.sched.available_nodes, self.sched.running_tasks)
        print("Completion:", self.sched.task_completion_flag)


    def mark_slow(self):
        self.slow_status = True
    
    def remove_slow(self):
        self.slow_status = False

