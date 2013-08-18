import threading
import time
import json as js
import unicodedata
from pprint import pprint


def get_reverse(incoming_edges):
    """ Return a dictionary of the reverse of the dependency
        dictionary. The returned dictionary has key that 
        represents is a dependency of the tasks in the list value """
    rev_json = {}
   
    for key, val in incoming_edges.items():
        for node in val:
            rev_json[node] = rev_json.get(node, []) + [key]
   
    for key in incoming_edges: # For tasks that gets rid of no dependencies
        if key not in rev_json:
            rev_json[key] = []

    return rev_json

def get_nondependent_task_ids(incoming_edges):
    """ Return a list of tasks with no dependencies """
    S = []
    for key, val in incoming_edges.items():
        if not val:
            S.append(key)
    return S
 
def get_dependencies(json_string):
    """ Return a dictionary of tasks and their
        dependencies as defined in the json file """
    d = js.loads(json_string)['tasks']    
    all_dependencies = {} # Maps tasaks to their dependencies
    for task in d:
        dependencies = task['dependencies']
        key = task['id']
        all_dependencies[key] = dependencies 
    return all_dependencies

def get_runtimes(json_string):
    """ Return a dictionary of task ids and their
        runtimes as defined in the json file """

    d = js.loads(json_string)['tasks']
    all_runtimes = {}
    for task in d:
        all_runtimes[task['id']] = task['runtime']
    return all_runtimes

def tasks_satisfied_dependencies(json_string):
    """ Return a dictionary of task ids that haven't 
    yet satisfied its dependencies """

    all_tasks = js.loads(json_string)['tasks']
    tasks_unsatisfied = {}
    for task in all_tasks:
        tasks_unsatisfied[task['id']] = False
    return tasks_unsatisfied

def get_tasks_order():
    """ Calculate the order in which tasks should be excecuted based
    on their runtime and dependencies """

    current_task = no_incoming_edges.pop(0)

    if not all_dependencies[current_task] and satisfied[current_task]:

        order_of_nodes.append(unicodedata.normalize('NFKD', current_task).encode('ascii','ignore'))

        print "Running task " + current_task + " with runtime " + str(all_runtimes[current_task])
        time.sleep(all_runtimes[current_task])
        print "------------------------------> Finished running task " + current_task

        for neighbour in dependencies_to_remove[current_task]: # We remove all dependencies for future tasks
            all_dependencies[neighbour].remove(current_task) # We remove a dependency

            if not all_dependencies[neighbour]: # all dependencies satisfied (removed)
                if neighbour not in no_incoming_edges:
                    no_incoming_edges.append(neighbour)
                satisfied[neighbour] = True

                new_task_thread = threading.Thread(target=get_tasks_order)
                new_task_thread.start()
                threads.append(new_task_thread)

        time.sleep(0)

if __name__ == "__main__":

    s = open("tasks.json", 'r').read()

    all_dependencies = get_dependencies(s) # task -> dependencies
    all_runtimes = get_runtimes(s) # tasks -> runtimes (an integer value)
    dependencies_to_remove = get_reverse(all_dependencies) # tasks that remove their dependencies
    no_incoming_edges =  get_nondependent_task_ids(all_dependencies) # starts as tasks that have no dependencies

    satisfied = tasks_satisfied_dependencies(s)

    # set all tasks with no dependencies to satisfied ie no dependencies
    for task in no_incoming_edges:
        satisfied[task] = True

    order_of_nodes = []
    threads = []

    # parallelize the first tasks with no dependencies.
    # once those tasks remove themselves from other tasks that depend on them,
    # the next tasks get a thread called on them
    while no_incoming_edges:

        task_thread = threading.Thread(target=get_tasks_order)
        task_thread.start()
        threads.append(task_thread)

    for thread in threads:
        thread.join()

    print order_of_nodes
