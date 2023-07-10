#!/usr/bin/env python3
import re
import sys


def process_manager(manager, m):
    pid = int(m.group("pid"))
    state = m.group("state")
    if state == "START":
        manager["pid"] = pid
        manager["start"] = int(m.group("time"))
        manager["end"] = 0
    elif state == "END":
        manager["end"] = int(m.group("time"))


def process_task(tasks, m):
    taskid = int(m.group("taskid"))
    state = m.group("state")
    if taskid not in tasks:
        tasks[taskid] = {"wait": 0, "start": 0, "worker": "", "end": 0,
                         "category": "", "state_args": ""}
    if state == "WAITING":
        tasks[taskid]["wait"] = int(m.group("time"))
        tasks[taskid]["category"] = m.group("state_args").split()[0]
    if state == "RUNNING":
        tasks[taskid]["start"] = int(m.group("time"))
        tasks[taskid]["worker"] = m.group("state_args").split()[0]
    if state == "DONE":
        tasks[taskid]["end"] = int(m.group("time"))
        state_args = m.group("state_args").split()
        if (int(state_args[1])) != 0:
            tasks[taskid]["state_args"] = "FAILED"
        else:
            tasks[taskid]["state_args"] = state_args[0]

    if state == "CANCELED":
        if tasks[taskid]["start"] > 0:
            tasks[taskid]["end"] = int(m.group("time"))

    tasks[taskid]["state"] = state


def process_worker(workers, m):
    state = m.group("state")
    host = m.group("host")
    if state == "CONNECTION":
        workers[host] = {"workerid": m.group("workerid"),
                         "start": int(m.group("time")),
                         "end": 0,
                         "state_args": ""}
    if state == "DISCONNECTION":
        workers[host]["end"] = int(m.group("time"))
        workers[host]["state_args"] = m.group("state_args")

    workers[host]["state"] = state


def parse(logfile):
    tasks = {}
    workers = {}
    manager = {}
    p_task = re.compile(r"(?P<time>\d+)\s+"
                        r"(?P<pid>\d+)\s+"
                        r"TASK\s+(?P<taskid>\d+)"
                        r"\s+(?P<state>\S+)\s*"
                        r"(?P<state_args>.+)?", re.X)
    p_worker = re.compile(r"(?P<time>\d+)\s+"
                          r"(?P<pid>\d+)\s+"
                          r"WORKER\s+"
                          r"(?P<workerid>\S+)\s+"
                          r"(?P<host>\S+)\s+"
                          r"(?P<state>CONNECTION|DISCONNECTION)\s*"
                          r"(?P<state_args>.+)?", re.X)

    p_manager = re.compile(r"(?P<time>\d+)\s+"
                           r"(?P<pid>\d+)\s+"
                           r"MANAGER\s+"
                           r"(?P<state>START|END)\s+", re.X)

    with open(logfile, 'r') as log:
        for line in log:
            if m := p_manager.match(line):
                process_manager(manager, m)
            elif m := p_task.match(line):
                process_task(tasks, m)
            elif m := p_worker.match(line):
                process_worker(workers, m)

    return [manager, workers, tasks]


if __name__ == '__main__':
    manager, workers, tasks = parse(sys.argv[1])
    print(f"Manager runtime: {(manager['end']-manager['start'])/1e6}s")
    print()
    print("Task ID;Category;Worker;Waiting;Runtime;Status;Extra")
    for taskid in tasks:
        t = tasks[taskid]
        wait = max(0, (t['start']-t['wait'])/1e6)
        runtime = max(0, (t['end']-t['start'])/1e6)
        print(f"{taskid};{t['category']};{t['worker']};"
              f"{wait};{runtime};{t['state']};'{t['state_args']}'")

    print()
    print("Worker;Runtime;Status;Extra")
    for worker in workers:
        w = workers[worker]
        print(f"{worker};{(w['end']-w['start'])/1e6};"
              f"{w['state']};'{w['state_args']}'")
