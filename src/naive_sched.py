"""
This file contains the naive scheduler class - that will not do any speculation. 
"""
import threading
import time

class NaiveScheduler:
    def __init__(self, tasks):
        self.failed_tasks = [] # prioritized over slow tasks
        self.slow_tasks = []
        # self.tasks = {"task_id": {"type": "map"}}
        self.tasks = tasks
        self.id = "naive"
        self.map_tasks_remaining = 0
        self.available_nodes = []
        self.task_id_list = []
        for i in self.tasks:
            self.task_id_list.append(i)
        self.threads = {} # maps worker to the thread. 

        # TODO: for now, I am assuming that 1 machine can execute a single task
        # we can increase/decrease this limit but it would impact how we add and remove things
        # from the available nodes list

    def set_node_cluster(self, node_cluster):
        self.node_cluster = node_cluster
        for node_id in self.node_cluster.node_pool:
            self.available_nodes.append(node_id)
        print(self.available_nodes)

    def add_task(self, key, value):
        print("new task has been added")
        self.tasks[key] = value
        self.task_id_list.append(key)

    def print_stats(self, time_taken):
        print("Total time taken by the naive scheduler: {:.4f}".format(time_taken))
        print("Number of tasks completed: ", len(self.threads))
        print("Number of nodes used: ", len(self.node_cluster.node_pool))
        #TODO: print other important stats here

    def mark_map_task_finished(self):
        self.map_tasks_remaining -= 1

    def assign_tasks(self):
        start_time = time.time()
        """Function to assign map tasks to workers."""
        while True:
            # get the first task and try to perform it
            print(self.task_id_list, self.tasks)
            task = self.task_id_list[0]
            task_assigned = 0
            if self.tasks[task]["type"] == "map":
                print(self.available_nodes)
                node_id = self.available_nodes[-1]
                worker = self.node_cluster.node_pool[node_id]
                self.available_nodes.pop() # make this worker unavailable now
                task_assigned = 1
                self.map_tasks_remaining += 1 # this worker is starting a map task
                thread = threading.Thread(target=worker.execute_map_task(task)) 
                self.threads[node_id] = thread
                print("DEBUG", node_id, self.threads)
                thread.start()
            elif self.tasks[task]["type"] == "copy":
                node_id = self.available_nodes[-1]
                worker = self.node_cluster.node_pool[node_id]
                self.available_nodes.pop() # make this worker unavailable now
                task_assigned = 1
                thread = threading.Thread(target=worker.execute_copy_task(task))
                self.threads[node_id] = thread
                thread.start()
            elif self.tasks[task]["type"] == "sort":
                node_id = self.available_nodes[-1]
                worker = self.node_cluster.node_pool[node_id]
                self.available_nodes.pop() # make this worker unavailable now
                task_assigned = 1
                thread = threading.Thread(target=worker.execute_sort_task(task)) 
                self.threads[node_id] = thread
                thread.start()
            elif self.tasks[task]["type"] == "reduce":
                # make sure all the map tasks have finished first
                if self.map_tasks_remaining > 0:
                    task_assigned = 0
                else:
                    node_id = self.available_nodes[-1]
                    worker = self.node_cluster.node_pool[node_id]
                    task_assigned = 1
                    self.available_nodes.pop() # make this worker unavailable now
                    thread = threading.Thread(target=worker.execute_reduce_task(task))
                    self.threads[node_id] = thread
                    thread.start()

            if task_assigned == 1:
                # task has been assigned to a worker node
                self.tasks.pop(task)
                print("hereeee", self.task_id_list, self.tasks)
                self.task_id_list.pop(0)
                print("hereeee", self.task_id_list, self.tasks)
            else:
                # place it in the end so it does not block other tasks
                self.task_id_list.pop(0)
                self.task_id_list.append(task)
                print("in here")
            print(self.task_id_list, self.tasks)
            # check if some task has finished
            threads_done = 0
            for i in self.threads:
                if self.threads[i].is_alive() == False:
                    # add it back to the list of available nodes
                    self.available_nodes.append(i)
                    threads_done += 1
            
            if len(self.tasks) == 0:
                # we are done with what we had, and do not have anything more to do
                break
        end_time = time.time()
        # join all the threads in here
        print(self.threads)
        for i in self.threads:
            self.threads[i].join()
        
        time_taken = end_time - start_time

        self.print_stats(time_taken)
        


            

