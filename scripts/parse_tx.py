#!/usr/bin/env python3


class TransactionLog:
    def __init__(self, jobid, filename):
        from pathlib import Path
        self.manager = {"pid": None, "start": 0, "end": 0}
        self.tasks = {}
        self.workers = {}
        self.jobid = jobid
        self.filename = Path(filename)
        self.parse(filename)

    def process_manager(self, m):
        state = m.group("state")
        if state == "START":
            self.manager.update(pid=int(m.group("pid")),
                                start=int(m.group("time"))/1e6)
        elif state == "END":
            self.manager["end"] = int(m.group("time"))/1e6

    def process_task(self, m):
        taskid = int(m.group("taskid"))
        state = m.group("state")
        args = m["state_args"].split() if m["state_args"] else None
        if taskid not in self.tasks:
            self.tasks[taskid] = {"wait": 0, "start": 0, "worker": "",
                                  "end": 0, "category": "", "state_args": ""}
        if state == "WAITING":
            self.tasks[taskid].update(wait=int(m.group("time"))/1e6,
                                      category=args[0])
        elif state == "RUNNING":
            self.tasks[taskid].update(start=int(m.group("time"))/1e6,
                                      worker=args[0])
        elif state == "DONE":
            state_args = "FAILED" if int(args[1]) != 0 else args[0]
            self.tasks[taskid].update(end=int(m.group("time"))/1e6,
                                      state_args=state_args)

        elif state == "CANCELED" and self.tasks[taskid]["start"] > 0:
            self.tasks[taskid]["end"] = int(m.group("time"))/1e6

        self.tasks[taskid]["state"] = state

    def process_worker(self, m):
        state = m.group("state")
        host = m.group("host")
        if state == "CONNECTION":
            self.workers[host] = {"workerid": m.group("workerid"),
                                  "start": int(m.group("time"))/1e6,
                                  "end": 0,
                                  "state_args": ""}
        if state == "DISCONNECTION":
            self.workers[host].update(end=int(m.group("time"))/1e6,
                                      state_args=m.group("state_args"))

        self.workers[host]["state"] = state

    def parse(self, logfile):
        import lzma
        import re
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

        suffix = self.filename.suffix
        myopen = lzma.open if suffix == '.xz' else open
        mode = 'rt' if suffix == '.xz' else 'r'
        with myopen(logfile, mode) as log:
            for line in log:
                if m := p_manager.match(line):
                    self.process_manager(m)
                elif m := p_task.match(line):
                    self.process_task(m)
                elif m := p_worker.match(line):
                    self.process_worker(m)

    def manager_runtime(self):
        if (self.manager['end']-self.manager['start'] < 1):
            raise Exception(f"Manager runtime muito pequeno em {self.filename}: {self.jobid}")
        return (self.manager['end']-self.manager['start'])

    def print_tasks(self):
        print("JobId;Task ID;Category;Worker;Waiting;Runtime;Status;Extra")
        for taskid in self.tasks:
            t = self.tasks[taskid]
            wait = max(0, (t['start']-t['wait']))
            runtime = max(0, (t['end']-t['start']))
            print(f"{self.jobid};{taskid};{t['category']};{t['worker']};"
                  f"{wait};{runtime};{t['state']};'{t['state_args']}'")

    def print_workers(self):
        print("JobId;Worker;Runtime;Status;Extra")
        for worker in self.workers:
            w = self.workers[worker]
            print(f"{self.jobid};{worker};{(w['end']-w['start'])};"
                  f"{w['state']};'{w['state_args']}'")


if __name__ == '__main__':
    import sys
    from pathlib import Path
    txfile = Path(sys.argv[1])
    tx = TransactionLog(txfile.parent.stem, txfile)
    print(f"Manager runtime: {tx.manager_runtime()}s")
    print()
    tx.print_tasks()
    print()
    tx.print_workers()
