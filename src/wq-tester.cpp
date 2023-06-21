#include <cstdio>
#include <cstdlib>
#include <random>
#include <unistd.h>
#include <vector>
extern "C" {
#include "cctools/work_queue.h"
#include "cctools/debug.h"
}

#include "wq_utils.h"

using namespace std;

struct work_queue_task* make_task(int ntask, int input_size, int run_time,
                                  int output_size, double chance, char *category)
{
	char output_file[128], tasklog[128], input_file[128], command[256];

    sprintf(input_file, "input.%d", 0);
    sprintf(output_file, "output.%d", ntask);
    sprintf(tasklog, "task-%03d.log", ntask);
    sprintf(command, "./wq-work infile %d outfile %d %d %f &> task.log",
            input_size, output_size, run_time, chance);

    struct work_queue_task *t = work_queue_task_create(command);
    work_queue_task_specify_file(t, "wq-work", "wq-work", WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
    work_queue_task_specify_file(t, input_file, "infile", WORK_QUEUE_INPUT, WORK_QUEUE_CACHE);
    work_queue_task_specify_file(t, tasklog, "task.log", WORK_QUEUE_OUTPUT, WORK_QUEUE_WATCH);
    work_queue_task_specify_cores(t,1);

    if(category && strlen(category) > 0)
       work_queue_task_specify_category(t, category);
	return t;
}

int main(int argc, char *argv[]) {

    debug_flags_set("wq");
    debug_config_file("manager-debug.log");
    char c;
    int num_tasks = 10;
    double chance = 0.1; // 10% of chance of running for double the time.
    int base_runtime = 20;
    int wait = 60;
    double fast_abort = 0.0;
    bool backup_tasks = false;
    double spec_exec = 0;
    while((c = getopt(argc, argv, "n:c:r:t:f:b:s:"))!=-1) {
        switch (c){
            case 'n':
                num_tasks = atoi(optarg); break;
            case 'c':
                chance = atof(optarg); break;
            case 'r':
                base_runtime = atoi(optarg); break;
            case 't':
                wait = atoi(optarg); break;
            case 'f':
                fast_abort = atof(optarg); break;
            case 'b':
                backup_tasks = true;
            case 's':
                spec_exec = atof(optarg); break;
            default:
                printf("Usage: %s [-n ntasks] [-c chance] [-r runtime] [-t nsecs] [-f multi] [-b] [-s multi]\n", argv[0]);
        }
    }
    if(backup_tasks && spec_exec>0.0) fatal("can't enable backup tasks and speculative execution together");
    if (wait < 10) wait = 10;
    struct work_queue *q = work_queue_create(WORK_QUEUE_DEFAULT_PORT);
    if(!q) fatal("couldn't listen on any port!");
    work_queue_specify_transactions_log(q, "transactions.log");
    printf("listening on port %d...\n", work_queue_port(q));
    if(fast_abort>0.0){
        printf("Activating fast abort with multiplier %f\n", fast_abort);
        work_queue_activate_fast_abort(q, fast_abort);
    }
    if(spec_exec>0.){
        printf("Activating spectultive execution with multiplier %f\n", spec_exec);
    }
    printf("Using base runtime %d\n", base_runtime);
    printf("Chance of straggler task %.2f\n", chance);
    if (backup_tasks) printf("Activating backup tasks.\n");
    std::unordered_map<int, int> backups;
    std::unordered_map<int, work_queue_task*> tasks;
    tasks.reserve(num_tasks);
    printf("Submitting %d tasks.\n", num_tasks);
    for (int i=0; i < num_tasks; i++){
        struct work_queue_task* t = make_task(i, 800, base_runtime, 500, chance, nullptr);
        int taskid;
        if (backup_tasks)
            taskid = wq_submit_clone(q, t, backups, -10);
        else if (spec_exec>0.0)
            taskid = wq_submit_speculative(q, t, tasks);
        else
            taskid = work_queue_submit(q, t);
        printf("Submitted task %d with command '%s'.\n", taskid, t->command_line);
    }
    struct work_queue_task *t = nullptr;
    struct work_queue_stats s;
    double avg_time = 0.;
    vector<pair<int,double>> task_times;
    fflush(stdout);
    while(!work_queue_empty(q)) {
        if(backup_tasks)
            t = wq_wait_clone(q, 60, backups);
        else if (spec_exec>0.0)
            t = wq_wait_speculative(q, 60, spec_exec, backups, tasks);
        else
            t = work_queue_wait(q, 60);
        if(t) {
            task_times.push_back(make_pair(t->taskid, t->time_workers_execute_last));
            work_queue_task_delete(t);
        }
        work_queue_get_stats(q, &s);
        if (s.tasks_done > 0)
            avg_time = s.time_workers_execute/double(s.tasks_done);
        printf(
            "|      Workers     ||               Tasks                 |\n"
            "| Connected | Busy || Waiting | Running | Done | Avg Time |\n"
            "| %9d | %4d || %7d | %7d | %4d | %8.2f |\n\n",
            s.workers_connected, s.workers_busy,
            s.tasks_waiting, s.tasks_running, s.tasks_done,
            avg_time*1e-6);
    fflush(stdout);
    }
    work_queue_shut_down_workers(q, 0);
    work_queue_delete(q);
    printf("Task execution times:\n");
    for (auto& p: task_times){
        printf("Taskid %d: %6.2f\n", p.first, p.second*1e-6);
    }
    printf("Done\n");
    return 0;
}
