"""
This file contains the hadoop scheduler class. 
"""

class NaiveScheduler:
    def __init__(self, node_cluster):
        self.node_cluster = node_cluster
        self.failed_tasks = []
        self.slow_tasks = []

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