"""
This file contains the hadoop scheduler class. 
"""
import threading
import time
from log import InfoLogger

def form_log(msg):
    t = time.time()
    InfoLogger.info(f"LOGGING - SCHED - [{t}] - {msg}")

class HadoopScheduler:
    def __init__(self, tasks, threshold):
        self.task_id = len(tasks) - 1
        self.regular_tasks = {}
        self.duplicate_tasks = {}
        self.running_tasks = {}
        self.regular_tasks = tasks
        self.waiting_reduce = {}
        self.threshold = threshold # to decide which task is slow
        self.id = "hadoop"
        self.map_tasks_remaining = []
        self.available_nodes = []
        self.node_progress_stats = {}
        self.worker_threads = {}
        self.task_completion_flag = {}
        self.num_completion = 0
        self.map_num = len(tasks)
        self.lock = threading.Lock()
        self.generated = False

    
    def set_node_cluster(self, node_cluster):
        print("SET")
        self.node_cluster = node_cluster
        for node_id in self.node_cluster.node_pool:
            self.available_nodes.append(node_id)
            self.node_progress_stats[node_id] = {"working": True, # the nodes are not working by default
                                                 "task_id": -1, # set it to the apt task id
                                                "progress_score": -1,
                                                "time_to_completion": 0,
                                                "progress_rate": 0,
                                                "dup": 0}
    
    def update_node_progress(self, id, progress_score, T, task_id, dup):
        """Function to change the progress stats of a node"""
        # ID = node ID
        with self.lock:
            self.node_progress_stats[id]["progress_score"] = progress_score
            self.node_progress_stats[id]["task_id"] = task_id
            self.node_progress_stats[id]["dup"] = dup

    def assign_tasks(self):
        while(True):
            with self.lock:
                if len(self.regular_tasks) != 0:
                    for tid in self.regular_tasks:
                        task = self.regular_tasks[tid]
                        if len(self.available_nodes) != 0:
                            node_id = self.available_nodes.pop()
                            worker = self.node_cluster.node_pool[node_id]
                            self.regular_tasks.pop(tid)
                            self.running_tasks[tid] = [task, 0]
                            if task["type"] == "map":
                                form_log(f"ASSIGN-MAP: [TASK:{tid}] : [NODE:{node_id}]")
                                thread = threading.Thread(target=worker.execute_map_task, args=(tid,0))
                                self.worker_threads[node_id] = thread
                                thread.start()
                            elif task["type"] == "reduce":
                                form_log(f"ASSIGN-RED: [TASK:{tid}] : [NODE:{node_id}]")
                                thread = threading.Thread(target=worker.execute_reduce_task, args=(tid,0))
                                self.worker_threads[node_id] = thread
                                thread.start()
                            break
                else:
                    if len(self.available_nodes) != 0:
                        for nid in self.node_progress_stats:
                            if self.node_progress_stats[nid]["progress_score"] < self.threshold and self.node_progress_stats[nid]["task_id"] != -1 and len(self.running_tasks) != 0:   
                                tid = self.node_progress_stats[nid]["task_id"] #task_id
                                if tid in self.duplicate_tasks or tid not in self.running_tasks:
                                    break
                                task = self.running_tasks[tid][0]
                                node_id = self.available_nodes.pop()
                                worker = self.node_cluster.node_pool[node_id]
                                self.duplicate_tasks[tid] = [task,0]
                                if task["type"] == "map":
                                    thread = threading.Thread(target=worker.execute_map_task, args=(tid,1))
                                    self.worker_threads[node_id] = thread
                                    form_log(f"DUP-MAP-BEGIN: [ORIG_NODE:{nid}] : [NODE:{node_id}] : [TASK:{tid}]")
                                    thread.start()
                                elif task["type"] == "reduce":
                                    thread = threading.Thread(target=worker.execute_reduce_task, args=(tid,1))
                                    self.worker_threads[node_id] = thread
                                    form_log(f"DUP-RED-BEGIN: [ORIG_NODE:{nid}] : [NODE:{node_id}] : [TASK:{tid}]")
                                    thread.start()
                                break
                if len(self.regular_tasks) == 0 and len(self.running_tasks) == 0 and len(self.duplicate_tasks) == 0 and len(self.waiting_reduce) == 0:
                    break


    def generate_next_tasks(self):
        while(True):
            with self.lock:
                if self.num_completion == self.map_num and not self.generated:# PUSH REDUCE WHEN MAP finishes
                    assert(len(self.waiting_reduce) == self.num_completion)
                    for tid in self.waiting_reduce: # COPY
                        self.regular_tasks[tid] = self.waiting_reduce[tid]
                    self.waiting_reduce.clear()
                    self.generated = True
                for tid in self.running_tasks:
                    if tid in self.task_completion_flag:
                        time.sleep(0.1)  
                        if self.running_tasks[tid][0]["type"] == "map":
                            self.task_id += 1
                            self.num_completion += 1
                            self.waiting_reduce[self.task_id] = {"type": "reduce"} # adding in a temp dict
                            form_log(f"GEN-RED: [TASK:{self.task_id}]")
                        if self.running_tasks[tid][1] == 0:
                            form_log(f"DUP-SUCCESS : [TASK:{tid}]")
                        self.running_tasks.pop(tid)
                        if tid in self.duplicate_tasks:
                            if self.duplicate_tasks[tid][1] == 0:
                                form_log(f"DUP-WASTE : [TASK:{tid}]")
                            self.duplicate_tasks.pop(tid)
                        break
                if len(self.regular_tasks) == 0 and len(self.running_tasks) == 0 and len(self.duplicate_tasks) == 0 and len(self.waiting_reduce) == 0:
                    assert(len(self.task_completion_flag) == self.task_id + 1)
                    print("final:",self.available_nodes)
                    break