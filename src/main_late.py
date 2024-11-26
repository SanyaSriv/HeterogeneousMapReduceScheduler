import late_sched
import hadoop_node
import threading
# making a node cluster here
# first number must be even
node_cluster = hadoop_node.NodeCluster(10, 0.5, 10, 8, 6, 5)

# defining the tasks here
tasks = {"0": {"type": "map"},
         "1": {"type": "map"},
         "2": {"type": "map"},
         "3": {"type": "map"},
         "4": {"type": "map"},
         "5": {"type": "map"},
         "6": {"type": "map"},
         "7": {"type": "map"},
         "8": {"type": "map"},
         "9": {"type": "map"},
         "10": {"type": "map"}}

# making the scheduler instance here
sched = late_sched.LateScheduler(tasks, 0.9)

node_cluster.set_scheduler(sched)
node_cluster.init_homogeneous_nodes(2, [0, 5])
sched.set_node_cluster(node_cluster)


# Create threads for both functions
scheduler_thread = threading.Thread(target=sched.assign_tasks)
gen_thread = threading.Thread(target=sched.generate_next_tasks)

scheduler_thread.start()
gen_thread.start()

scheduler_thread.join()
gen_thread.join()