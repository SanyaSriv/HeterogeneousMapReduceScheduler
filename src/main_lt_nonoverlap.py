import late_wo_overlap
import node_wo_overlap
import threading
# making a node cluster here
node_cluster = node_wo_overlap.NodeCluster(10, 0.5, 10, 8, 6, 5)

tasks = {}
# defining the tasks here
for i in range(11):
    tasks[i] = {"type": "map"}


# making the scheduler instance here
sched = late_wo_overlap.LateScheduler(tasks, 0.9)

node_cluster.set_scheduler(sched)
# the 1 parameter will generate random rates according to the hardocded range
node_cluster.init_homogeneous_nodes(2, [0, 1], 1, [])
sched.set_node_cluster(node_cluster)


# Create threads for both functions
scheduler_thread = threading.Thread(target=sched.assign_tasks)
gen_thread = threading.Thread(target=sched.generate_next_tasks)

scheduler_thread.start()
gen_thread.start()

scheduler_thread.join()
gen_thread.join()