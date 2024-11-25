"""
This file contains the hadoop scheduler class. 
"""
import threading
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler("log.txt"),
        logging.StreamHandler() 
    ]
)

def form_log(msg):
    t = time.time()
    logging.info(f"LOGGING - SCHED - [{t}] - {msg}")

class HadoopScheduler:
    def __init__(self, tasks, threshold):
        self.task_id = len(tasks) - 1
        self.regular_tasks = {}
        self.duplicate_tasks = {}
        self.running_tasks = {}
        self.regular_tasks = tasks
        self.threshold = threshold # to decide which task is slow
        self.id = "hadoop"
        self.map_tasks_remaining = []
        self.available_nodes = []
        self.node_progress_stats = {}
        self.worker_threads = {}
        self.task_completion_flag = {}
        self.num_completion = 0
        self.map_num = len(tasks)
        self.lock_stats = threading.Lock()
        self.lock_tasks = threading.Lock()
        self.lock_nodes = threading.Lock()

    
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
        with self.lock_stats:
            self.node_progress_stats[id]["progress_score"] = progress_score
            self.node_progress_stats[id]["task_id"] = task_id
            self.node_progress_stats[id]["dup"] = dup

    def assign_tasks(self):
        while(True):
            if len(self.regular_tasks) != 0:
                for tid in self.regular_tasks:
                    task = self.regular_tasks[tid]
                    with self.lock_nodes:
                        if len(self.available_nodes) != 0:
                            node_id = self.available_nodes.pop()
                            worker = self.node_cluster.node_pool[node_id]
                            with self.lock_tasks:
                                self.regular_tasks.pop(tid)
                                self.running_tasks[tid] = [task, 0]
                            if task["type"] == "map":
                                form_log(f"ASSIGN-MAP: [TASK:{self.task_id}] : [TID:{tid}] : [NODE:{node_id}]")
                                thread = threading.Thread(target=worker.execute_map_task, args=(tid,0))
                                self.worker_threads[node_id] = thread
                                thread.start()
                            elif task["type"] == "reduce":
                                form_log(f"ASSIGN-RED: [TASK:{self.task_id}] : [TID:{tid}] : [NODE:{node_id}]")
                                thread = threading.Thread(target=worker.execute_reduce_task, args=(tid,0))
                                self.worker_threads[node_id] = thread
                                thread.start()
                    break
            else:
                if len(self.available_nodes) != 0:
                    for nid in self.node_progress_stats:
                        #  NID = node ID
                        with self.lock_tasks: 
                            if self.node_progress_stats[nid]["progress_score"] < self.threshold and self.node_progress_stats[nid]["task_id"] != -1 and len(self.running_tasks) != 0:   
                                tid = self.node_progress_stats[nid]["task_id"] #task_id
                                if tid in self.duplicate_tasks:
                                    break
                                task = self.running_tasks[tid][0]
                                self.duplicate_tasks[tid] = [task,0]
                                with self.lock_nodes:
                                    node_id = self.available_nodes.pop()
                                    worker = self.node_cluster.node_pool[node_id]
                                if task["type"] == "map":
                                    thread = threading.Thread(target=worker.execute_map_task, args=(tid,1))
                                    self.worker_threads[node_id] = thread
                                    form_log(f"DUP-MAP-BEGIN: [ORIG_NODE:{nid}] : [NODE:{node_id}] : [TASK:{tid}]")
                                    thread.start()
                                elif task["type"] == "reduce":
                                    thread = threading.Thread(target=worker.execute_reduce_task, args=(tid,1))
                                    self.worker_threads[node_id] = thread
                                    form_log(f"DUP-RED-BEGIN: [ORIG_NODE:{nid}] : [NODE:{node_id}] : [TASK:{tid}]]")
                                    thread.start()
                        break
            if len(self.regular_tasks) == 0 and len(self.running_tasks) == 0 and len(self.duplicate_tasks) == 0:
                break


    def generate_next_tasks(self):
        while(True):
            for tid in self.running_tasks:
                with self.lock_tasks:
                    if self.running_tasks[tid][1] == 1 or (tid in self.duplicate_tasks and self.duplicate_tasks[tid][1] == 1):
                        time.sleep(1)  
                        if self.running_tasks[tid][0]["type"] == "map":
                            self.task_id += 1
                            self.num_completion += 1
                            self.regular_tasks[self.task_id] = {"type": "reduce"}
                            form_log(f"GEN-RED: [TASK:{self.task_id}]")
                        if self.running_tasks[tid][1] == 0:
                            form_log(f"DUP-SUCCESS : [TASK:{tid}]")
                        self.running_tasks.pop(tid)
                        if tid in self.duplicate_tasks:
                            if self.duplicate_tasks[tid][1] == 0:
                                form_log(f"DUP-WASTE : [TASK:{tid}]")
                            self.duplicate_tasks.pop(tid)
                break
            if len(self.regular_tasks) == 0 and len(self.running_tasks) == 0 and len(self.duplicate_tasks) == 0:
                assert(len(self.task_completion_flag) == self.task_id + 1)
                print("final:",self.available_nodes)
                break
