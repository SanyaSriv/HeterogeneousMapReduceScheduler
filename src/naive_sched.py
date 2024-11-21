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

    def assign_map_tasks(self):
        """Function to assign map tasks to workers."""
        pass

    def assign_red_tasks(self):
        """Function to assign reduce tasks to workers."""
        pass

    def assign_failed_tasks(self):
        pass

    def assign_slow_tasks(self):
        pass