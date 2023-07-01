#include <cstdio>
#include <cstdlib>
#include <random>
#include <unistd.h>
#include <vector>
#include <chrono>

extern "C" {
#include "cctools/work_queue.h"
#include "cctools/debug.h"
}

#include "wq_utils.h"

using namespace std;

struct work_queue_task* make_dummy_task(int ntask, int input_size, int run_time,
                                  int output_size, double chance, char *category)
{
	char tasklog[128], input_file[128], command[256];

    sprintf(input_file, "input.%d", 0);
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

struct work_queue_task* make_sort_task(int ntask, char *category)
{
	char output_file[128], tasklog[128], input_file[128], command[256];

    sprintf(input_file, "input%d", ntask);
    sprintf(output_file, "output.%d", ntask);
    sprintf(tasklog, "task-%03d.log", ntask);
    sprintf(command, "sort -g --parallel=1 -o outfile infile &> task.log");

    struct work_queue_task *t = work_queue_task_create(command);
    work_queue_task_specify_file(t, input_file, "infile", WORK_QUEUE_INPUT, WORK_QUEUE_NOCACHE);
    work_queue_task_specify_file(t, tasklog, "task.log", WORK_QUEUE_OUTPUT, WORK_QUEUE_WATCH);
    work_queue_task_specify_file(t, output_file, "outfile", WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);
    work_queue_task_specify_cores(t,1);

    if(category && strlen(category) > 0)
       work_queue_task_specify_category(t, category);
	return t;
}


int main(int argc, char *argv[]) {

    using std::chrono::steady_clock, std::chrono::duration_cast, std::chrono::duration;
    using std::chrono::seconds, std::chrono::minutes, std::chrono::hours;

    auto start = steady_clock::now();
    debug_flags_set("wq");
    debug_config_file("manager-debug.log");
    char c;
    int num_tasks = 10;
    double chance = 0.1; // 10% of chance of running for double the time.
    int base_runtime = 20;
    int wait = 60;
    double fast_abort = 0.0;
    bool backup_tasks = false;
    double spec_exec = -1.0;
    while((c = getopt(argc, argv, "n:c:r:t:f:bs:"))!=-1) {
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
                backup_tasks = true; break;
            case 's':
                spec_exec = atof(optarg); break;
            default:
                printf("Usage: %s [-n ntasks] [-c chance] [-r runtime] [-t nsecs] [-f multi] [-b] [-s multi]\n", argv[0]);
                exit(1);
        }
    }
    bool dummy_task = false;
    if (optind < argc){
        if (string(argv[optind]) == "dummy") dummy_task = true;
    }
    if(backup_tasks && spec_exec>0.0) fatal("Can't enable backup tasks and speculative execution together\n");
    if (wait < 10) wait = 10;
    printf("Using base runtime %d\n", base_runtime);
    printf("Chance of straggler task %.2f\n", chance);

    if (backup_tasks){
        printf("Activating backup tasks.\n");
        spec_exec = 0.0;
    } else if (spec_exec>0.0){
        printf("Activating speculative execution with multiplier %f\n", spec_exec);
    } else
        printf("Disabling speculative execution\n");

    wq_speculative_queue* wq = wq_create(WORK_QUEUE_DEFAULT_PORT, spec_exec);
    if (!wq) fatal("Can't create queue!");
    if (fast_abort > 0.0) {
        printf("Activating fast abort with multiplier %f\n", fast_abort);
        wq_specify_fast_abort(wq, fast_abort);
    }

    printf("Submitting %d tasks.\n", num_tasks);
    for (int i=0; i < num_tasks; i++){
        struct work_queue_task* t;
        if (dummy_task)
           t = make_dummy_task(i, 800, base_runtime, 500, chance, nullptr);
        else
           t = make_sort_task(i, nullptr);
        int taskid = wq_submit(wq, t);
        printf("Submitted task %d with command '%s'.\n", taskid, t->command_line);
    }

    struct work_queue_stats s;
    double avg_time = 0.;
    vector<pair<int,double>> task_times;
    printf("Waiting workers\n");
    fflush(stdout);
    struct work_queue* inner_q = wq_q(wq);
    while(!work_queue_empty(inner_q)) {
        struct work_queue_task *t = wq_wait(wq, wait);
        if(t) {
            if(t->result == WORK_QUEUE_RESULT_SUCCESS) {
                task_times.push_back(make_pair(t->taskid, t->time_workers_execute_last));
                work_queue_task_delete(t);
            } else {
                printf("Task %d failed with error %s!\n", t->taskid, work_queue_result_str(t->result));
            }
        }

        work_queue_get_stats(inner_q, &s);
        if (s.tasks_done > 0)
            avg_time = s.time_workers_execute/double(s.tasks_done);
        printf(
            "|      Workers     ||               Tasks                 |\n"
            "| Connected | Busy || Waiting | Running | Done | Avg Time |\n"
            "| %9d | %4d || %7d | %7d | %4d | %8.2f |\n",
            s.workers_connected, s.workers_busy,
            s.tasks_waiting, s.tasks_running, s.tasks_done,
            avg_time*1e-6);
        printf("\033[3A");
        fflush(stdout);
    }
    printf("\033[3B");
    work_queue_shut_down_workers(inner_q, 0);
    wq_delete(wq);
    printf("\nTask execution times:\n");
    for (auto& p: task_times){
        printf("Taskid %d: %6.2f\n", p.first, p.second*1e-6);
    }
    printf("Done\n");
    auto program_time = steady_clock::now() - start;
    printf("Total program time: %f secs\n",
           duration<double>(program_time).count());
    return 0;
}
