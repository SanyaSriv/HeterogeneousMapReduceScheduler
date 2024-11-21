"""
This file contains the naive scheduler class - that will not do any speculation. 
"""
import threading
import time

class NaiveScheduler:
    def __init__(self, node_cluster, tasks, threshold):
        self.node_cluster = node_cluster
        self.failed_tasks = [] # prioritized over slow tasks
        self.slow_tasks = []
        # self.tasks = ["task_id": {"type": "map"}]
        self.tasks = tasks
        self.threshold = threshold # to decide which task is slow
        self.id = "naive"
        self.map_tasks_remaining = []
        self.available_nodes = []
        for node_id in self.node_cluster.node_pool:
            self.available_nodes.append(node_id)
        self.threads = {} # maps worker to the thread. 

        # TODO: for now, I am assuming that 1 machine can execute a single task
        # we can increase/decrease this limit but it would impact how we add and remove things
        # from the available nodes list

    def add_task(self, task):
        self.tasks.append(task)

    def print_stats(self, time_taken):
        print("Total time taken by the naive scheduler: {.4f}".format(time_taken))
        print("Number of tasks completed: ", len(self.threads))
        print("Number of nodes used: ", len(self.node_cluster.node_pool))
        #TODO: print other important stats here
        
    def assign_tasks(self):
        start_time = time.time()
        """Function to assign map tasks to workers."""
        while True:
            # get the first task and try to perform it
            task = self.tasks[0]
            task_assigned = 0
            if self.task[task]["type"] == "map":
                worker = self.node_cluster.node_pool[self.available_nodes[-1]]
                self.available_nodes.pop() # make this worker unavailable now
                task_assigned = 1
                self.map_tasks_remaining.append(worker) # this worker is starting a map task
                thread = threading.Thread(target=worker.execute_map_tasks())  # Each thread sleeps for a different time
                self.threads[worker] = thread
                thread.start()
            elif self.task[task]["type"] == "copy":
                worker = self.node_cluster.node_pool[self.available_nodes[-1]]
                self.available_nodes.pop() # make this worker unavailable now
                task_assigned = 1
                thread = threading.Thread(target=worker.execute_copy_tasks())  # Each thread sleeps for a different time
                self.threads[worker] = thread
                thread.start()
            elif self.task[task]["type"] == "sort":
                worker = self.node_cluster.node_pool[self.available_nodes[-1]]
                self.available_nodes.pop() # make this worker unavailable now
                task_assigned = 1
                thread = threading.Thread(target=worker.execute_sort_tasks())  # Each thread sleeps for a different time
                self.threads[worker] = thread
                thread.start()
            elif self.task[task]["type"] == "reduce":
                # make sure all the map tasks have finished first
                if len(self.map_tasks_remaining) > 0:
                    task_assigned = 0
                else:
                    worker = self.node_cluster.node_pool[self.available_nodes[-1]]
                    task_assigned = 1
                    self.available_nodes.pop() # make this worker unavailable now
                    thread = threading.Thread(target=worker.execute_reduce_tasks())  # Each thread sleeps for a different time
                    self.threads[worker] = thread
                    thread.start()

            if task_assigned == 1:
                # task has been assigned to a worker node
                self.tasks.pop(0)
            else:
                # place it in the end so it does not block other tasks
                self.tasks.pop(0)
                self.tasks.append(task)
            
            # check if some task has finished
            threads_done = 0
            for i in self.threads:
                if self.threads[i].is_alive() == False:
                    # add it back to the list of available nodes
                    self.available_nodes.append(i)
                    threads_done += 1
                    if i in self.map_tasks_remaining:
                        self.map_tasks_remaining.remove(i)
            
            if threads_done == self.threads and len(self.tasks) == 0:
                # we are done with what we had, and do not have anything more to do
                break
        end_time = time.time()
        # join all the threads in here
        for i in self.threads:
            self.threads[i].join()
        
        time_taken = end_time - start_time

        print("Time taken by the naive scheduler: ")
        


            

