"""
This file contains the naive scheduler class - that will do no specu. 
"""

class NaiveScheduler:
    def __init__(self, node_cluster, tasks, threshold):
        self.node_cluster = node_cluster
        self.failed_tasks = [] # prioritized over slow tasks
        self.slow_tasks = []
        self.tasks = tasks
        self.threshold = threshold # to decide which task is slow
        self.id = "naive"

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
                    if self.map_tasks_remaining > 0:
                        continue;
                    pass
                    # make sure all the map tasks have finished first
                    # we need to launch a thread
                    # assign a job to one of the workers