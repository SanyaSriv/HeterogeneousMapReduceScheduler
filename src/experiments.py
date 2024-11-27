import subprocess
import os
import node
import node_wo_overlap
import hadoop_node
import naive_sched
import hadoop_sched
import hadoop_wo_overlap
import late_sched
import late_wo_overlap
import threading
import time

# Make all the node cluster classes in here
NUM_EXPERIMENTS = 3

# Experiment 1 --> 2 stragglers - 1 node straggler + 10 nodes + 15 tasks [small gap in speeds between stragg;er node and regular node]
# Experiment 2 --> 2 stragglers - 1 node straggler + 10 nodes + 15 tasks [increased the speed difference between straggler nodes and reglar nodes]
# Experiment 3 --> 4 stragglers - 2 node straggler + 10 nodes + 15 tasks [small gap in speeds between stragg;er node and regular node]
# Experiment 3 --> 4 stragglers - 2 node straggler + 10 nodes + 15 tasks [increased gap in speeds between stragg;er node and regular node]
# repeat the above but with number of nodes = number of tasks

HadoopNodeClusters = []
HadoopNoOverlapNodeClusters = []
NaiveNodeClusters = []
LateNodeClusters = []
LateNoOverlapNodeClusters = []

def run(sched, node_cluster, num_stragglers, list_stragglers, speed_random, speed_list):
    sched.set_node_cluster(node_cluster)
    node_cluster.set_scheduler(sched)
    # the 1 parameter will generate random rates according to the hardocded range
    node_cluster.init_homogeneous_nodes(num_stragglers, list_stragglers, speed_random, speed_list)
    sched.set_node_cluster(node_cluster)

    # Create threads for both functions
    scheduler_thread = threading.Thread(target=sched.assign_tasks)
    gen_thread = threading.Thread(target=sched.generate_next_tasks)

    scheduler_thread.start()
    gen_thread.start()

    scheduler_thread.join()
    gen_thread.join()

    time.sleep(2)

# initializing all parameters
for i in range(0, NUM_EXPERIMENTS):
    class_inst = node.NodeCluster(10, 0.5, 4, 2, 3, 1)
    NaiveNodeClusters.append(class_inst)

    class_inst = hadoop_node.NodeCluster(10, 0.5, 4, 2, 3, 1)
    HadoopNodeClusters.append(class_inst)

    class_inst = hadoop_node.NodeCluster(10, 0.5, 4, 2, 3, 1)
    LateNodeClusters.append(class_inst)

    class_inst = node_wo_overlap.NodeCluster(10, 0.5, 4, 2, 3, 1)
    HadoopNoOverlapNodeClusters.append(class_inst)

    class_inst = node_wo_overlap.NodeCluster(10, 0.5, 4, 2, 3, 1)
    LateNoOverlapNodeClusters.append(class_inst)

# Running the experiments here
# Running the first experiment
HadoopTaskList = {}
HadoopNoOverlapTaskList = {}
NaiveTaskList = {}
LateTaskList = {}
LateNoOverlapTaskList = {}

for i in range(0, 10): # 15 tasks
    to_add_key = "{}".format(i)
    to_add_value = {"type": "map"}

    HadoopTaskList[to_add_key] = to_add_value
    HadoopNoOverlapTaskList[to_add_key] = to_add_value
    NaiveTaskList[to_add_key] = to_add_value
    LateTaskList[to_add_key] = to_add_value
    LateNoOverlapTaskList[to_add_key] = to_add_value

# speed_list = [0.7, 0.8, 1.5, 1.4, 2.0, 1.6, 1.23, 1.35, 1.90, 1.75] # experiment 1 + 5
#  speed_list = [0.3, 0.1, 1.5, 1.4, 2.0, 1.6, 1.23, 1.35, 1.90, 1.75] # experiment 2 + 6
# speed_list = [0.7, 0.8, 1.5, 1.4, 2.0, 1.6, 0.65, 0.52, 1.90, 1.75] # experiment 3 + 7
speed_list =   [0.3, 0.1, 1.5, 1.4, 2.0, 1.6, 0.38, 0.21, 1.90, 1.75] # experiment 4 + 8

sched = hadoop_sched.HadoopScheduler(HadoopTaskList, 0.3)
run(sched, HadoopNodeClusters[0], 4, [0, 1, 6, 7], 0, speed_list)
subprocess.run(["mv", "log.txt", "HadoopExperiment8.txt"], check=True)
subprocess.run(['python3', 'interpret_log.py', '--log_file', 'HadoopExperiment8.txt', '--output_dir', 'experiment_graphs'], check=True)
subprocess.run(['mv', 'experiment_graphs/copy_sort_red.png', 'experiment_graphs/HadoopExperiment8.png'], check=True)
subprocess.run(['touch', 'log.txt'], check=True)
subprocess.run(["mv", "HadoopExperiment8.txt", "experiment_logs/HadoopExperiment8.txt"], check=True)
time.sleep(5)

# sched = hadoop_wo_overlap.HadoopScheduler(HadoopNoOverlapTaskList, 0.3)
# run(sched, HadoopNoOverlapNodeClusters[0], 4, [0, 1, 6, 7], 0, speed_list)
# # move the log.txt file to a diff location
# subprocess.run(["mv", "log.txt", "HadoopNoOverlapExperiment8.txt"], check=True)
# subprocess.run(['python3', 'interpret_log.py', '--log_file', 'HadoopNoOverlapExperiment8.txt', '--output_dir', 'experiment_graphs'], check=True)
# subprocess.run(['mv', 'experiment_graphs/copy_sort_red.png', 'experiment_graphs/HadoopNoOverlapExperiment8.png'], check=True)
# subprocess.run(["mv", "HadoopNoOverlapExperiment8.txt", "experiment_logs/HadoopNoOverlapExperiment8.txt"], check=True)
# time.sleep(5)

# sched = naive_sched.NaiveScheduler(NaiveTaskList)
# run(sched, NaiveNodeClusters[0], 4, [0, 1, 6, 7], 0, speed_list)
# # move the log.txt file to a diff location
# subprocess.run(["mv", "log.txt", "NaiveExperiment8.txt"], check=True)
# subprocess.run(['python3', 'interpret_log.py', '--log_file', 'NaiveExperiment8.txt', '--output_dir', 'experiment_graphs'], check=True)
# subprocess.run(['mv', 'experiment_graphs/copy_sort_red.png', 'experiment_graphs/NaiveExperiment8.png'], check=True)
# subprocess.run(["mv", "NaiveExperiment8.txt", "experiment_logs/NaiveExperiment8.txt"], check=True)

# sched = late_sched.LateScheduler(LateTaskList, 0)
# run(sched, LateNodeClusters[0], 4, [0, 1, 6, 7], 0, speed_list)
# # move the log.txt file to a diff location
# subprocess.run(["mv", "log.txt", "LateExperiment8.txt"], check=True)
# subprocess.run(['python3', 'interpret_log.py', '--log_file', 'LateExperiment8.txt', '--output_dir', 'experiment_graphs'], check=True)
# subprocess.run(['mv', 'experiment_graphs/copy_sort_red.png', 'experiment_graphs/LateExperiment8.png'], check=True)
# subprocess.run(["mv", "LateExperiment8.txt", "experiment_logs/LateExperiment8.txt"], check=True)

# sched = late_wo_overlap.LateScheduler(LateNoOverlapTaskList, 0)
# run(sched, LateNoOverlapNodeClusters[0], 4, [0, 1, 6, 7], 0, speed_list)
# # move the log.txt file to a diff location
# subprocess.run(["mv", "log.txt", "LateNoOverlapExperiment8.txt"], check=True)
# subprocess.run(['python3', 'interpret_log.py', '--log_file', 'LateNoOverlapExperiment8.txt', '--output_dir', 'experiment_graphs'], check=True)
# subprocess.run(['mv', 'experiment_graphs/copy_sort_red.png', 'experiment_graphs/LateNoOverlapExperiment8.png'], check=True)
# subprocess.run(["mv", "LateNoOverlapExperiment8.txt", "experiment_logs/LateNoOverlapExperiment8.txt"], check=True)

