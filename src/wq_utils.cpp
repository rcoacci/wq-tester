
#include "wq_utils.h"
extern "C" {
#include "cctools/debug.h"
}
#include <cstring>
#include <string>

int wq_submit_clone(struct work_queue* q, struct work_queue_task* t, std::unordered_map<int,int>& backup_tasks, int backup_prio){
    struct work_queue_task* bkp = work_queue_task_clone(t);
    work_queue_task_specify_category(bkp, "backup");
    work_queue_task_specify_priority(bkp, backup_prio);

    int original_task = work_queue_submit(q, t);
    int bkp_task = work_queue_submit(q, bkp);
    debug(D_WQ, "Submitted backup (speculative) task %d for task %d.", bkp_task, original_task);
    // Add two mappings, one from the original to the clone
    backup_tasks[original_task] = bkp_task;
    // And from the clone to the original.
    backup_tasks[bkp_task] = original_task;
    return original_task;
}

struct work_queue_task *wq_wait_clone(struct work_queue *q, int timeout, std::unordered_map<int,int>& backup_tasks){
    struct work_queue_task* t = work_queue_wait(q, timeout);
    if(t && t->result == WORK_QUEUE_RESULT_SUCCESS){
        if (std::strcmp(t->category, "backup")==0)
            debug(D_WQ, "Backup (speculative) task %d finished before the regular task. Tag: '%s', cmd: '%s'", t->taskid, t->tag, t->command_line);
        else
            debug(D_WQ, "Regular task %d finished before the backup task. Tag: '%s', cmd: '%s'", t->taskid, t->tag, t->command_line);
        auto it = backup_tasks.find(t->taskid);
        if(it != backup_tasks.end()){
            debug(D_WQ, "Found task %d pointing to task %d in the map. Cancelling %d.", it->first, it->second, it->second);
            struct work_queue_task* other = work_queue_cancel_by_taskid(q, it->second);
            if (other) work_queue_task_delete(other);
            else debug(D_WQ, "Warning, the mirror task was not returned from WQ!");
            // Find the mirror task mapping
            auto it_other = backup_tasks.find(it->second);
            // Remove both tasks from the map.
            backup_tasks.erase(it);
            backup_tasks.erase(it_other);
        } else
            debug(D_WQ, "Warning, task %d not found on map!", t->taskid);
    }
    return t;
}

int wq_submit_speculative(struct work_queue* q, struct work_queue_task* t, std::unordered_map<int, work_queue_task*>& tasks){
    int taskid = work_queue_submit(q, t);
    tasks[taskid] = t;
    return taskid;
}


static int wq_spec_check_replica(struct work_queue * q, struct work_queue_task* t,
                                 std::unordered_map<int,int>& replicas) {
    int replica = -1;
    auto it = replicas.find(t->taskid);
    if(it != replicas.end()){
        debug(D_WQ, "Found task %d pointing to task %d in the map. Cancelling %d.", it->first, it->second, it->second);
        replica = it->second;
        struct work_queue_task* other = work_queue_cancel_by_taskid(q, it->second);
        if (other) work_queue_task_delete(other);
        else debug(D_WQ, "Warning, the mirror task was not returned from WQ!");
        // Remove both tasks from the map.
        replicas.erase(it);
        // Find the mirror task mapping
        replicas.erase(replicas.find(it->second));
    } // else ok, not all tasks will have replicas
    return replica;
}

static void wq_spec_submit_replicas(struct work_queue *q, double spec_mult,
                             std::unordered_map<int,int>& replicas,
                             std::unordered_map<int, work_queue_task*>& tasks){
    struct work_queue_stats s;
    work_queue_get_stats(q, &s);
    if (s.tasks_done > 5){
        uint64_t average = (s.time_workers_execute_good + s.time_send_good + s.time_receive_good) / s.tasks_done;
        if (average>1){
            uint64_t now = timestamp_get();
            uint64_t spec_trigger = average*spec_mult;
            debug(D_WQ, "Current speculative trigger is %f secs.", spec_trigger*1.e-6);
            for (auto& p: tasks){
                struct work_queue_task* t1 = p.second;
                // Ignore tasks that are not running;
                if(work_queue_task_state(q, t1->taskid) != WORK_QUEUE_TASK_RUNNING) continue;
                // Ignore tasks that already have replicas
                if(replicas.find(t1->taskid) != replicas.end()) continue;
                uint64_t runtime = now - t1->time_when_commit_start;
                if(runtime >= spec_trigger){
                    struct work_queue_task* bkp = work_queue_task_clone(t1);
                    work_queue_task_specify_category(bkp, "backup");
                    work_queue_task_specify_priority(bkp, t1->priority+1); // Increase priority so we get scheduled ASAP
                    int bkp_task = work_queue_submit(q, bkp);
                    debug(D_WQ, "Task %d is taking too long (%f of %f avg), submitting speculative task %d.",
                          t1->taskid, runtime*1e-6, average*1e-6, bkp_task);
                    // Add two mappings, one from the original to the clone
                    replicas[t1->taskid] = bkp_task;
                    // And from the clone to the original.
                    replicas[bkp_task] = t1->taskid;
                }
            }
        }
    }
}
struct work_queue_task *wq_wait_speculative(struct work_queue *q, int timeout, double spec_mult,
                                            std::unordered_map<int,int>& replicas,
                                            std::unordered_map<int, work_queue_task*>& tasks){
    struct work_queue_task* t = work_queue_wait(q, timeout);
    if(t && t->result == WORK_QUEUE_RESULT_SUCCESS){
        int replica = wq_spec_check_replica(q, t, replicas);
        if (std::string(t->category) == "backup")
            tasks.erase(tasks.find(replica));
        else
            tasks.erase(tasks.find(t->taskid));
    }
    wq_spec_submit_replicas(q, spec_mult, replicas, tasks);
    return t;
}
