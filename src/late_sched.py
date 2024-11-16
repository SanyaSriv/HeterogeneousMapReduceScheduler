"""
This file contains the intelligent LATE scheduler design.
"""

class LateScheduler:
    def __init__(self, node_cluster, tasks, slow_N_threshold):
        self.node_cluster = node_cluster
        self.failed_tasks = [] # prioritized over slow tasks
        self.slow_tasks = []
        self.tasks = tasks
        self.slow_N_Threshold = slow_N_threshold

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