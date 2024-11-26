import matplotlib.pyplot as plt
import time
import argparse
import re

# defining some global REs here that will help in sorting through the logs
PARAM_MATCH =  r"\[([a-zA-Z]+):([0-9.]+)\]"

def processing_logs_stage1(raw_log):
    sorted_logs = {"node": [],
                   "sched": []}
    f = open(raw_log, 'r')
    for line in f:
        if "LOGGING - NODE" in line:
            sorted_logs["node"].append(line.replace("LOGGING - NODE - ", ""))
        elif "LOGGING - SCHED" in line:
            sorted_logs["sched"].append(line.replace("LOGGING - SCHED - ", ""))
            print(line.replace("LOGGING - SCHED - ", ""))

    f.close()
    return sorted_logs

def processing_logs_stage2(log):
    event_dictionary = {}
    for event in log:
        event_list = event.split(" ")
        print("event list = ", event_list)
        time_stamp = event_list[0].replace("[", "").replace("]", "")
        event_type = event_list[2].replace(":", "")
        event_parameters = {}
        for p in range(3, len(event_list)):
            pp = re.findall(PARAM_MATCH, event_list[p])
            if len(pp) > 0:
                event_parameters[pp[0][0]] = pp[0][1]
        if event_type not in event_dictionary:
            event_dictionary[event_type] = [{"timestamp": time_stamp,
                                            "params": event_parameters}]
        else:
            event_dictionary[event_type].append({"timestamp": time_stamp,
                                            "params": event_parameters})
    return event_dictionary

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_file") # input log file
    parser.add_argument("--output_dir") # output directory where all the plots would be saved
    args = parser.parse_args()

    # this should be a dictionary
    log_file_sep = processing_logs_stage1(args.log_file)
    node_logs = processing_logs_stage2(log_file_sep["node"])
    sched_logs = processing_logs_stage2(log_file_sep["sched"])
    straggler_node_list = [int(x["params"]["NODE"]) for x in node_logs["STRAGGLER"]]
    node_list = [int(x["params"]["NODE"]) for x in node_logs["CREATE-NODE"]]
    print(straggler_node_list)
    # MAKING ALL THE NODE EVENT GRAPHS

    # First graph - for map task assignments: 
    # getting important data in here
    timestamps = [float(x['timestamp']) for x in sched_logs["ASSIGN-MAP"]]
    nodes = [int(x['params']['NODE']) for x in sched_logs["ASSIGN-MAP"]]
    tasks = [x['params']['TASK'] for x in sched_logs["ASSIGN-MAP"]]
    plt.figure(figsize=(10, 6))
    for i in range(len(timestamps)):
        plt.barh(nodes[i], 0.02, left=timestamps[i], color="skyblue", edgecolor="black", height=0.4)
        plt.text(timestamps[i] + 0.004, nodes[i], f"{tasks[i]}", va='center', fontsize=12)
    plt.xlabel("Timestamp")
    plt.ylabel("Node ID")
    plt.title("Map Task Assignment Timeline (Straggler Nodes Highlighted)")
    plt.grid(axis='x', linestyle='-')
    node_colors = ["red" if node in straggler_node_list else "black" for node in node_list]
    for tick, color in zip(plt.yticks(node_list)[1], node_colors):
        tick.set_color(color)
    plt.tight_layout()
    plt.savefig(args.output_dir + "/map_assignment.png")

    #second graph - scheduler speculation graph
    #Third graph -- showing the beginning of task execution and also the end of it
    # extracting data about when map tasks begin
    timestamps_begin = [float(x['timestamp']) for x in node_logs["BEGIN-MAP"]]
    nodes1 = [int(x['params']['NODE']) for x in node_logs["BEGIN-MAP"]]
    tasks1 = [int(x['params']['TASK']) for x in node_logs["BEGIN-MAP"]]
    dup1 = [int(x['params']['DUP']) for x in node_logs["BEGIN-MAP"]]

    # extracting data about when map tasks end
    timestamps_end = [float(x['timestamp']) for x in node_logs["DONE-MAP"]]
    nodes2 = [int(x['params']['NODE']) for x in node_logs["DONE-MAP"]]
    tasks2 = [int(x['params']['TASK']) for x in node_logs["DONE-MAP"]]
    dup2 = [int(x['params']['DUP']) for x in node_logs["DONE-MAP"]]

    # extracting data about whether any map tasks get aborted midway
    if "ABORT-REDUNDANT-MAP" in node_logs:
        redundant_abort_timestamp = [float(x['timestamp']) for x in node_logs["ABORT-REDUNDANT-MAP"]]
        nodes_redundant = [int(x['params']['NODE']) for x in node_logs["ABORT-REDUNDANT-MAP"]]
        tasks_redundant = [int(x['params']['TASK']) for x in node_logs["ABORT-REDUNDANT-MAP"]]
        dup_redundant = [int(x['params']['DUP']) for x in node_logs["ABORT-REDUNDANT-MAP"]]
        score_redundant = [float(x['params']['STAT']) for x in node_logs["ABORT-REDUNDANT-MAP"]]
    else:
        redundant_abort_timestamp = []
        nodes_redundant = []
        tasks_redundant = []
        dup_redundant = []
        score_redundant = []

    time_stamp_dict_begin = {}
    time_stamp_dict_end = {}
    redundant_time_stamp_dict = {}
    score = {}

    for i in range(0, len(nodes1)):
        time_stamp_dict_begin[(nodes1[i], tasks1[i], dup1[i])] = timestamps_begin[i]
    
    for i in range(0, len(nodes2)):
        time_stamp_dict_end[(nodes2[i], tasks2[i], dup2[i])] = timestamps_end[i]
    
    for i in range(0, len(nodes_redundant)):
        redundant_time_stamp_dict[(nodes_redundant[i], tasks_redundant[i], dup_redundant[i])] = redundant_abort_timestamp[i]
        score[(nodes_redundant[i], tasks_redundant[i], dup_redundant[i])] = score_redundant[i]

    plt.figure(figsize=(10, 6))
    for i in range(len(timestamps_begin)):
        score_display = 0
        pair = (nodes1[i], tasks1[i], dup1[i])
        if pair in time_stamp_dict_end:
            time_end = time_stamp_dict_end[pair]
        elif pair in redundant_time_stamp_dict:
            time_end = redundant_time_stamp_dict[pair]
            score_display = 1
        else:
            print("NO END TIME FOUND ----------")
        if dup1[i] == 1:
            # mark these duplicate tasks in red
            c1 = "red"
            c2 = "black"
        else:
            c1 = "darkblue"
            c2 = "white"
        if score_display == 0:
            to_disp = tasks1[i]
        else:
            to_disp = "{} | {:.2f}".format(tasks1[i], score[pair])
        plt.barh(nodes1[i], time_end - timestamps_begin[i], left=timestamps_begin[i], color=c1, edgecolor="black", height=0.4)
        plt.text(timestamps_begin[i] + 0.004, nodes1[i], to_disp, va='center', fontsize=12, color=c2)
    # plt.xlabel("Timestamp")
    # plt.ylabel("Node ID")
    # plt.title("Map Task Begin Timeline (Straggler Nodes Highlighted)")
    # plt.grid(axis='x', linestyle='-')
    # node_colors = ["red" if node in straggler_node_list else "black" for node in node_list]
    # for tick, color in zip(plt.yticks(node_list)[1], node_colors):
    #     tick.set_color(color)
    # plt.tight_layout()
    # plt.savefig(args.output_dir + "/map.png")


    #Third graph -- representing all the data about copy, sort, adn reduce tasks
    # extracting data about when copy tasks begin
    cpy_timestamps_begin = [float(x['timestamp']) for x in node_logs["BEGIN-COPY"]]
    cpy_nodes1 = [int(x['params']['NODE']) for x in node_logs["BEGIN-COPY"]]
    cpy_tasks1 = [int(x['params']['TASK']) for x in node_logs["BEGIN-COPY"]]
    cpy_dup1 = [int(x['params']['DUP']) for x in node_logs["BEGIN-COPY"]]

    # extracting data about when copy tasks end
    cpy_timestamps_end = [float(x['timestamp']) for x in node_logs["DONE-COPY"]]
    cpy_nodes2 = [int(x['params']['NODE']) for x in node_logs["DONE-COPY"]]
    cpy_tasks2 = [int(x['params']['TASK']) for x in node_logs["DONE-COPY"]]
    cpy_dup2 = [int(x['params']['DUP']) for x in node_logs["DONE-COPY"]]

    # extracting data about whether any copy tasks get aborted midway
    if "ABORT-REDUNDANT-COPY" in node_logs:
        cpy_redundant_abort_timestamp = [float(x['timestamp']) for x in node_logs["ABORT-REDUNDANT-COPY"]]
        cpy_nodes_redundant = [int(x['params']['NODE']) for x in node_logs["ABORT-REDUNDANT-COPY"]]
        cpy_tasks_redundant = [int(x['params']['TASK']) for x in node_logs["ABORT-REDUNDANT-COPY"]]
        cpy_dup_redundant = [int(x['params']['DUP']) for x in node_logs["ABORT-REDUNDANT-COPY"]]
        score_redundant = [float(x['params']['STAT']) for x in node_logs["ABORT-REDUNDANT-COPY"]]
    else:
        cpy_redundant_abort_timestamp = []
        cpy_nodes_redundant = []
        cpy_tasks_redundant = []
        cpy_dup_redundant = []
        score_redundant = []

    cpy_time_stamp_dict_begin = {}
    cpy_time_stamp_dict_end = {}
    cpy_redundant_time_stamp_dict = {}
    score = {}

    for i in range(0, len(cpy_nodes1)):
        cpy_time_stamp_dict_begin[(cpy_nodes1[i], cpy_tasks1[i], cpy_dup1[i])] = cpy_timestamps_begin[i]
    
    for i in range(0, len(cpy_nodes2)):
        cpy_time_stamp_dict_end[(cpy_nodes2[i], cpy_tasks2[i], cpy_dup2[i])] = cpy_timestamps_end[i]
    
    for i in range(0, len(cpy_nodes_redundant)):
        cpy_redundant_time_stamp_dict[(cpy_nodes_redundant[i], cpy_tasks_redundant[i], cpy_dup_redundant[i])] = cpy_redundant_abort_timestamp[i]
        score[(cpy_nodes_redundant[i], cpy_tasks_redundant[i], cpy_dup_redundant[i])] = score_redundant[i]
    
    print(cpy_time_stamp_dict_begin)
    print(cpy_time_stamp_dict_end)
    print(cpy_redundant_time_stamp_dict)
    # plt.figure(figsize=(10, 6))
    for i in range(len(cpy_timestamps_begin)):
        score_display = 0
        pair = (cpy_nodes1[i], cpy_tasks1[i], cpy_dup1[i])
        if pair in cpy_time_stamp_dict_end:
            time_end = cpy_time_stamp_dict_end[pair]
        elif pair in cpy_redundant_time_stamp_dict:
            time_end = cpy_redundant_time_stamp_dict[pair]
            score_display = 1
        else:
            print("NO END TIME FOUND FOR COPY ----------")
        if cpy_dup1[i] == 1:
            # mark these duplicate tasks in red
            c = "red"
        else:
            c = "skyblue"
        if score_display == 0:
            to_disp = cpy_tasks1[i]
        else:
            to_disp = "{} | {:.2f}".format(cpy_tasks1[i], score[pair])
        plt.barh(cpy_nodes1[i], time_end - cpy_timestamps_begin[i], left=cpy_timestamps_begin[i], color=c, edgecolor="black", height=0.4)
        plt.text(cpy_timestamps_begin[i] + 0.004, cpy_nodes1[i], to_disp, va='center', fontsize=12)
    
    
    # computing the same things for sort because the copy - sort - and reduce phases are combined into 1 task
    # so unless we show them together, the speculation would look weird
    sort_timestamps_begin = [float(x['timestamp']) for x in node_logs["BEGIN-SORT"]]
    sort_nodes1 = [int(x['params']['NODE']) for x in node_logs["BEGIN-SORT"]]
    sort_tasks1 = [int(x['params']['TASK']) for x in node_logs["BEGIN-SORT"]]
    sort_dup1 = [int(x['params']['DUP']) for x in node_logs["BEGIN-SORT"]]

    # extracting data about when the sort tasks finish
    sort_timestamps_end = [float(x['timestamp']) for x in node_logs["DONE-SORT"]]
    sort_nodes2 = [int(x['params']['NODE']) for x in node_logs["DONE-SORT"]]
    sort_tasks2 = [int(x['params']['TASK']) for x in node_logs["DONE-SORT"]]
    sort_dup2 = [int(x['params']['DUP']) for x in node_logs["DONE-SORT"]]

    # extracting data about whether any sort task got aborted because of a speculation
    if "ABORT-REDUNDANT-SORT" in node_logs:
        sort_redundant_abort_timestamp = [float(x['timestamp']) for x in node_logs["ABORT-REDUNDANT-SORT"]]
        sort_nodes_redundant = [int(x['params']['NODE']) for x in node_logs["ABORT-REDUNDANT-SORT"]]
        sort_tasks_redundant = [int(x['params']['TASK']) for x in node_logs["ABORT-REDUNDANT-SORT"]]
        sort_dup_redundant = [int(x['params']['DUP']) for x in node_logs["ABORT-REDUNDANT-SORT"]]
        score_redundant = [float(x['params']['STAT']) for x in node_logs["ABORT-REDUNDANT-SORT"]]
    else:
        sort_redundant_abort_timestamp = []
        sort_nodes_redundant = []
        sort_tasks_redundant = []
        sort_dup_redundant = []
        score_redundant = []
    
    # making sort timestamp dictionaries in here
    sort_time_stamp_dict_begin = {}
    sort_time_stamp_dict_end = {}
    sort_redundant_time_stamp_dict = {}
    score = {}

    for i in range(0, len(sort_nodes1)):
        sort_time_stamp_dict_begin[(sort_nodes1[i], sort_tasks1[i], sort_dup1[i])] = sort_timestamps_begin[i]

    for i in range(0, len(sort_nodes2)):
        sort_time_stamp_dict_end[(sort_nodes2[i], sort_tasks2[i], sort_dup2[i])] = sort_timestamps_end[i]

    for i in range(0, len(sort_nodes_redundant)):
        sort_redundant_time_stamp_dict[(sort_nodes_redundant[i], sort_tasks_redundant[i], sort_dup_redundant[i])] = sort_redundant_abort_timestamp[i]
        score[(sort_nodes_redundant[i], sort_tasks_redundant[i], sort_dup_redundant[i])] = score_redundant[i]

    for i in range(0, len(sort_timestamps_begin)):
        score_display = 0
        pair = (sort_nodes1[i], sort_tasks1[i], sort_dup1[i])
        if pair in sort_time_stamp_dict_end:
            time_end = sort_time_stamp_dict_end[pair]
        elif pair in sort_redundant_time_stamp_dict:
            time_end = sort_redundant_time_stamp_dict[pair]
            score_display = 1
        else:
            print("NO END TIME FOUND FOR SORT PHASE")
        if sort_dup1[i] == 1:
            # mark speculative task in the sort phase with a different color
            c = "#ed8e98"
        else:
            c = "#bfe01b" 
        if score_display == 0:
            to_disp = sort_tasks1[i]
        else:
            to_disp = "{} | {:.2f}".format(sort_tasks1[i], score[pair])
        plt.barh(sort_nodes1[i], time_end - sort_timestamps_begin[i], left=sort_timestamps_begin[i], color=c, edgecolor="black", height=0.4)
        plt.text(sort_timestamps_begin[i]+0.004, sort_nodes1[i], to_disp, va="center", fontsize=12)
    
    # adding data about reduce tasks here
    red_timestamps_begin = [float(x['timestamp']) for x in node_logs["BEGIN-RED"]]
    red_nodes1 = [int(x['params']['NODE']) for x in node_logs["BEGIN-RED"]]
    red_tasks1 = [int(x['params']['TASK']) for x in node_logs["BEGIN-RED"]]
    red_dup1 = [int(x['params']['DUP']) for x in node_logs["BEGIN-RED"]]

    # extracting data about when the sort tasks finish
    red_timestamps_end = [float(x['timestamp']) for x in node_logs["DONE-RED"]]
    red_nodes2 = [int(x['params']['NODE']) for x in node_logs["DONE-RED"]]
    red_tasks2 = [int(x['params']['TASK']) for x in node_logs["DONE-RED"]]
    red_dup2 = [int(x['params']['DUP']) for x in node_logs["DONE-RED"]]

    # extracting data about whether any sort task got aborted because of a speculation
    if "ABORT-REDUNDANT-RED" in node_logs:
        red_redundant_abort_timestamp = [float(x['timestamp']) for x in node_logs["ABORT-REDUNDANT-RED"]]
        red_nodes_redundant = [int(x['params']['NODE']) for x in node_logs["ABORT-REDUNDANT-RED"]]
        red_tasks_redundant = [int(x['params']['TASK']) for x in node_logs["ABORT-REDUNDANT-RED"]]
        red_dup_redundant = [int(x['params']['DUP']) for x in node_logs["ABORT-REDUNDANT-RED"]]
        score_redundant = [float(x['params']['STAT']) for x in node_logs["ABORT-REDUNDANT-RED"]]
    else:
        red_redundant_abort_timestamp = []
        red_nodes_redundant = []
        red_tasks_redundant = []
        red_dup_redundant = []
        score_redundant = []
    
    # making sort timestamp dictionaries in here
    red_time_stamp_dict_begin = {}
    red_time_stamp_dict_end = {}
    red_redundant_time_stamp_dict = {}
    score = {}

    for i in range(0, len(red_nodes1)):
        red_time_stamp_dict_begin[(red_nodes1[i], red_tasks1[i], red_dup1[i])] = red_timestamps_begin[i]

    for i in range(0, len(red_nodes2)):
        red_time_stamp_dict_end[(red_nodes2[i], red_tasks2[i], red_dup2[i])] = red_timestamps_end[i]

    for i in range(0, len(red_nodes_redundant)):
        red_redundant_time_stamp_dict[(red_nodes_redundant[i], red_tasks_redundant[i], red_dup_redundant[i])] = red_redundant_abort_timestamp[i]
        score[(red_nodes_redundant[i], red_tasks_redundant[i], red_dup_redundant[i])] = score_redundant[i]

    print(red_time_stamp_dict_begin)
    print(red_time_stamp_dict_end)
    print("redd", red_redundant_time_stamp_dict)
    for i in range(0, len(red_timestamps_begin)):
        pair = (red_nodes1[i], red_tasks1[i], red_dup1[i])
        score_display = 0
        if pair in red_time_stamp_dict_end:
            time_end = red_time_stamp_dict_end[pair]
        elif pair in red_redundant_time_stamp_dict:
            time_end = red_redundant_time_stamp_dict[pair]
            score_display = 1
        else:
            print("NO END TIME FOUND FOR RED PHASE: ", pair)
        if red_dup1[i] == 1:
            # mark speculative task in the sort phase with a different color
            c = "#f2c2c7"
        else:
            c = "yellow" 
        if score_display == 0:
            to_disp = red_tasks1[i]
        else:
            to_disp = "{} | {:.2f}".format(red_tasks1[i], score[pair])
        plt.barh(red_nodes1[i], time_end - red_timestamps_begin[i], left=red_timestamps_begin[i], color=c, edgecolor="black", height=0.4)
        plt.text(red_timestamps_begin[i]+0.004, red_nodes1[i], to_disp, va="center", fontsize=12)
    
    # collecting all timestamps when a red generation happens
    red_gen_timestamps = [float(x['timestamp']) for x in sched_logs["GEN-RED"]]
    red_gen_taskid = [x['params']['TASK'] for x in sched_logs["GEN-RED"]]

    # for i in range(0, len(red_gen_timestamps)):
    #     plt.plot([red_gen_timestamps[i], red_gen_timestamps[i]], [0, 4], color='pink', linestyle='-', label=f"{red_gen_taskid[i]}")
    # UNCOMMENT IF IF YOU WANT TO SEE GENERATION
    # for ts, task in zip(red_gen_timestamps, red_gen_taskid):
    #     plt.axvline(x=ts, color='pink', linestyle='-', label=f"Event: {task}")

    # Set custom x-ticks at the event timestamps
    # plt.xticks(red_gen_timestamps)
    plt.xlabel("Timestamp")
    plt.ylabel("Node ID")
    plt.title("Copy-Sort-Reduce Timeline (Straggler Nodes Highlighted)")
    plt.grid(axis='x', linestyle='-')
    node_colors = ["red" if node in straggler_node_list else "black" for node in node_list]
    for tick, color in zip(plt.yticks(node_list)[1], node_colors):
        tick.set_color(color)
    plt.tight_layout()
    plt.savefig(args.output_dir + "/copy_sort_red.png")
    # MAKING ALL THE SCHED EVENT GRAPHS

if __name__ == "__main__":
    main()