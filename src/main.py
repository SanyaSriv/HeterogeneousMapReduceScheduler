import naive_sched
import node
import threading

# making a node cluster here
node_cluster = node.NodeCluster(6, 0.5, 5, 10, 10, 5)

# defining the tasks here
tasks = {"0": {"type": "map"},
         "1": {"type": "map"},
         "2": {"type": "map"},
         "3": {"type": "map"},
         "4": {"type": "map"},
         "5": {"type": "map"}}

# making the scheduler instance here
sched = naive_sched.NaiveScheduler(tasks)

sched.set_node_cluster(node_cluster)
node_cluster.set_scheduler(sched)
node_cluster.init_homogeneous_nodes(2, [0, 1])
sched.set_node_cluster(node_cluster)


# Create threads for both functions
scheduler_thread = threading.Thread(target=sched.assign_tasks)
gen_thread = threading.Thread(target=sched.generate_tasks)

scheduler_thread.start()
gen_thread.start()

scheduler_thread.join()
gen_thread.join()