"""
This file contains the naive scheduler class - that will not do any speculation. 
"""
import threading
import time
from log import InfoLogger

def form_log(msg):
    t = time.time()
    InfoLogger.info(f"LOGGING - SCHED - [{t}] - {msg}")

class NaiveScheduler:
    def __init__(self, tasks):
        self.task_id = len(tasks) - 1
        self.tasks = tasks
        self.id = "naive"
        self.threads = {} # maps worker to the thread. 
        self.running_task_status = {} 
        self.map_tasks = len(tasks) # all the tasks would be mpa to begin with
        self.regular_tasks_dict = {}
        self.regular_task_list = [] #list to just maintain the ordering
        
        for i in self.tasks:
            # will have all map tasks initially
            self.regular_task_list.append(i)
            self.regular_tasks_dict[i] = {"type": "map"}
       
        # for keeping track of node availability
        # 1 = node available | 0 = node not available
        self.node_availibility = {}
        
        # defining mutex locks here to make the implementation thread safe
        self.running_status_lock = threading.Lock()
    
    def set_node_cluster(self, node_cluster):
        self.node_cluster = node_cluster
        for node_id in self.node_cluster.node_pool:
            self.node_availibility[node_id] = 1

    def print_stats(self, time_taken):
        print("Total time taken by the naive scheduler: {:.4f}".format(time_taken))
        print("Number of tasks completed: ", len(self.threads))
        print("Number of nodes used: ", len(self.node_cluster.node_pool))

    def mark_task_finished(self, task_id):
        with self.running_status_lock:
            self.running_task_status[task_id]["run_stat"] = 1

    def create_task_running(self, task_id, task_type):
        with self.running_status_lock:
            self.running_task_status[task_id] = {"run_stat": 0,
                                             "type": task_type}

    def mark_node_available(self, node_id):
        self.node_availibility[node_id] = 1
        print("node has been marked as available")

    def generate_next_tasks(self):
        """
        Function to generate tasks. There would be no speculative tasks, so the only tasks generated
        would be copy-sort-reduce (task with 3 phases).
        """
        while True:
            tasks_to_rm = []
            with self.running_status_lock:
                for task_id in self.running_task_status:
                    if self.running_task_status[task_id]["run_stat"] == 1 and self.running_task_status[task_id]["type"] == "map":
                        # launch a copy task
                        self.task_id += 1
                        self.regular_tasks_dict[self.task_id] = {"type":"copy"}
                        self.regular_task_list.append(self.task_id)
                        tasks_to_rm.append(task_id)
                        form_log(f"GEN-RED: [TASK:{self.task_id}]")
                        # decrement the number of map tasks in the system
                        self.map_tasks -= 1
                    elif self.running_task_status[task_id]["run_stat"] == 1:
                        tasks_to_rm.append(task_id)
                
                # start removing the tasks that were finished
                for t in tasks_to_rm:
                    self.running_task_status.pop(t)

            # check if everything is done - if so then break out
            if len(self.regular_task_list) == 0 and len(self.running_task_status) == 0:
                break

    def assign_tasks(self):
        """Function to assign map tasks to workers."""
        while True:
            if len(self.regular_task_list) == 0:
                if len(self.running_task_status) == 0:
                    break
                else:
                    continue
            # get the first task and try to perform it
            task = self.regular_task_list[0]
            task_assigned = 0
            if self.regular_tasks_dict[task]["type"] == "map":
                # get the next available node
                sorted_avail_list = sorted(self.node_availibility.keys(), key=lambda k: self.node_availibility[k])
                node_id = sorted_avail_list[-1]
                if self.node_availibility[node_id] != 1:
                    continue # none of the nodes are available -- loop again
                
                worker = self.node_cluster.node_pool[node_id]
                self.node_availibility[node_id] = 0 # make this worker unavailable now
                task_assigned = 1
                form_log(f"ASSIGN-MAP: [TASK:{task}] : [NODE:{node_id}]")
                self.create_task_running(task, "map")
                thread = threading.Thread(target=worker.execute_map_task, args=(task,)) 
                self.threads[node_id] = thread
                thread.start()
            
            elif self.regular_tasks_dict[task]["type"] == "copy":
                # copy can begin before all map tasks are done but reduce would have to wait
                # get the next available node
                sorted_avail_list = sorted(self.node_availibility.keys(), key=lambda k: self.node_availibility[k])
                node_id = sorted_avail_list[-1]
                if self.node_availibility[node_id] != 1:
                    continue # none of the nodes are available -- loop again
                
                worker = self.node_cluster.node_pool[node_id]
                self.node_availibility[node_id] = 0 # make this worker unavailable now
                task_assigned = 1
                form_log(f"ASSIGN-RED: [TASK:{task}] : [NODE:{node_id}]")
                self.create_task_running(task, "copy")
                thread = threading.Thread(target=worker.execute_red_task, args=(task,)) 
                self.threads[node_id] = thread
                thread.start()

            if task_assigned == 1:
                # task has been assigned to a worker node
                self.regular_tasks_dict.pop(task)
                self.regular_task_list.pop(0)
            else:
                # place it in the end so it does not block other tasks
                self.regular_task_list.pop(0)
                self.regular_task_list.append(task)

            # check if everything is done - if so then break out
            if len(self.regular_task_list) == 0 and len(self.running_task_status) == 0:
                break