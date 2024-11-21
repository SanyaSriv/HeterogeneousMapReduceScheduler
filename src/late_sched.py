"""
This file contains the intelligent LATE scheduler design.
"""

class LateScheduler:
    def __init__(self, node_cluster, tasks, slow_N_threshold):
        self.node_cluster = node_cluster
        self.failed_tasks = [] # prioritized over slow tasks
        self.slow_tasks = []
        self.node_progress_stats = {} #TODO: I am not sure what to initialize this with
        # this should be a dictionary of tasks
        # self.tasks = {id: {"type": map, "assign": None}, id: {}, id: {}, ...}
        # once, we assign this task to a node, we will have to change the assign parameter
        self.tasks = tasks
        self.slow_N_Threshold = slow_N_threshold
        self.available_nodes = []
        self.id = "late"
        for node_id in self.node_cluster.node_pool:
            # TODO, I am not sure how to initialize these attributes
            self.node_progress_stats[node_id] = {"working": False, # the nodes are not working by default
                                                 "task_id": -1, # set it to the apt task id
                                                "progress_score": 0,
                                                "time_to_completion": 0,
                                                "progress_rate": 0}
            self.available_nodes.append(node_id)
        self.map_tasks_remaining = [] # TODO: updated it when you launch a task; decrement it when a map tasks finishes

    def update_node_progress(self, node_id, ticks_done, total_ticks):
        """Function to change the progress stats of a node"""
        progress_score = #TODO
        progress_rate = #TODO
        time_to_completion = (1 - progress_score) / progress_rate
        self.node_progress_stats[node_id]["time_to_completion"] = time_to_completion
        if time_to_completion <= self.slow_N_Threshold:
            # mark this as a slow node
            self.node_cluster.set_slow_status(node_id)
        elif time_to_completion > self.slow_N_Threshold:
            self.node_cluster.remove_slow_status(node_id)
            # TODO: confirm if this is the point where we add a task to the slow task pile
            self.slow_tasks = []
    
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