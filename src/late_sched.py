"""
This file contains the intelligent LATE scheduler design.
"""

class LateScheduler:
    def __init__(self, node_cluster, tasks, slow_N_threshold):
        self.node_cluster = node_cluster
        self.failed_tasks = [] # prioritized over slow tasks
        self.slow_tasks = []
        self.node_progress_stats = {} #TODO: I am not sure what to initialize this with
        self.tasks = tasks
        self.slow_N_Threshold = slow_N_threshold
        self.id = "late"
        for node_id in self.node_cluster.node_pool:
            # TODO, I am not sure how to initialize these attributes
            self.node_progress_stats[node_id] = {"progress_score": 0,
                                                "time_to_completion": 0,
                                                "progress_rate": 0}

    def update_node_progress(self, node_id, ticks_done, total_ticks):
        """Function to change the progress stats of a node"""
        progress_score = #TODO
        progress_rate = #TODO
        time_to_completion = (1 - progress_score) / progress_rate
        self.node_progress_stats[node_id]["time_to_completion"] = time_to_completion

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