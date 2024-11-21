import naive_sched
import node

# making a node cluster here
node_cluster = node.NodeCluster(5, 0.5, 5, 10, 10, 5)

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
node_cluster.init_homogeneous_nodes()
sched.set_node_cluster(node_cluster)

sched.assign_tasks()