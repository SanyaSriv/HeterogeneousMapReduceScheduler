"""
This file contains the hadoop scheduler class. 
"""

class HadoopScheduler:
    def __init__(self, node_cluster, tasks, threshold):
        self.node_cluster = node_cluster
        self.failed_tasks = [] # prioritized over slow tasks
        self.slow_tasks = []
        self.tasks = tasks
        self.threshold = threshold # to decide which task is slow
        self.id = "hadoop"
        self.map_tasks_remaining = []
        self.available_nodes = []
        self.node_progress_stats = {}
        for node_id in self.node_cluster.node_pool:
            # TODO, I am not sure how to initialize these attributes
            self.node_progress_stats[node_id] = {"working": False, # the nodes are not working by default
                                                 "task_id": -1, # set it to the apt task id
                                                "progress_score": 0,
                                                "time_to_completion": 0,
                                                "progress_rate": 0}
            self.available_nodes.append(node_id)
            
    def assign_tasks(self):
        """Function to assign map tasks to workers."""
        while (len(self.tasks) > 0):
            for task in self.tasks:
                if self.task[task]["type"] == "map":
                    pass
                    # we need to launch a thread
                    # assign a job to one of the available workers
                elif self.task[task]["type"] == "copy":
                    pass
                    # we need to launch a thread
                    # assign a job to one of the available workers
                elif self.task[task]["type"] == "sort":
                    pass
                    # we need to launch a thread
                    # assign a job to one of the available workers
                elif self.task[task]["type"] == "reduce":
                    if len(self.map_tasks_remaining) > 0:
                        continue
                    pass
                    # make sure all the map tasks have finished first
                    # we need to launch a thread
                    # assign a job to one of the workers
            
            # check if we have any late/failed tasks
            if len(self.slow_tasks) > 0:
                self.assign_slow_tasks()
            if len(self.failed_tasks) > 0:
                self.assign_failed_tasks()


    def assign_failed_tasks(self):
        pass

    def assign_slow_tasks(self):
        pass