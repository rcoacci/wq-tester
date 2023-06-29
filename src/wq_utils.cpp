
#include "wq_utils.h"
extern "C" {
#include "cctools/debug.h"
}
#include <cstring>
#include <string>
#include <unordered_map>


struct wq_speculative_queue {
    struct work_queue* q;
    int multiplier;
    int priority_change;
    std::unordered_map<int, work_queue_task*> tasks;
    std::unordered_map<int,int> replicas;

    wq_speculative_queue(struct work_queue* _q, int mult, int prio):
        q(_q),multiplier(mult), priority_change(prio), tasks(), replicas(){};
};

/*
**
** Speculative queue
**
*/
/**
 * If spec_mult > 0.0 creates a queue that replicates tasks when the execution time is >= average*multiplier
 * If spec_mult == 0 creates a queue that replicates tasks immediatly but makes them lower priority (a.k.a. backup tasks)
 * if spec_mult < 0.0  creates a queue that never replicates any task which is the reglar Work Queue behavior
 * */
wq_speculative_queue *wq_create(int port, double spec_mult) {
    struct work_queue* q = work_queue_create(port);
    if(!q) fatal("couldn't listen on any port!");
    work_queue_specify_transactions_log(q, "transactions.log");
    printf("listening on port %d...\n", work_queue_port(q));
    return new wq_speculative_queue(q, spec_mult, spec_mult == 0.0 ? -10 : 10);
}

void wq_specify_priority_change(wq_speculative_queue* wq, int change){
    wq->priority_change = change;
}

void wq_specify_fast_abort(wq_speculative_queue *wq, double multiplier) {
    if (multiplier > 0.0) {
        printf("Activating fast abort with multiplier %f\n", multiplier);
    } else {
        printf("Deactivating fast abort\n");
    }
    work_queue_activate_fast_abort(wq->q, multiplier);
}

int wq_submit(wq_speculative_queue* wq, work_queue_task* t){
    int taskid = work_queue_submit(wq->q, t);
    wq->tasks[taskid] = t;
    return taskid;
}

static void wq_spec_check_replica(wq_speculative_queue* wq, struct work_queue_task* t){
    // Does not check any replicas if multiplier is negative.
    // Allows for regular Work Queue behavior.
    if (wq->multiplier < 0.0) return;
    if(wq->replicas.count(t->taskid)>0){
        if (std::string(t->category) == "replica")
            debug(D_WQ, "Speculative task %d finished before the regular task. Tag: '%s', cmd: '%s'", t->taskid, t->tag, t->command_line);
        else
            debug(D_WQ, "Regular task %d finished before the speculative task. Tag: '%s', cmd: '%s'", t->taskid, t->tag, t->command_line);

        int replica_id = wq->replicas[t->taskid];
        debug(D_WQ, "Found task %d pointing to task %d in the map. Cancelling %d.", t->taskid, replica_id, replica_id);
        struct work_queue_task* other = work_queue_cancel_by_taskid(wq->q, replica_id);
        if (other) {
            work_queue_task_delete(other);
            // try to remove the mirror task from our main task list.
            // If it's a replica, it won't be found and nothing will be removed.
            wq->tasks.erase(replica_id);
        }
        else debug(D_WQ, "Warning, the mirror task was not returned from WQ!");
        // Remove both tasks from the map.
        wq->replicas.erase(t->taskid);
        // Erase the mirror task mapping
        wq->replicas.erase(replica_id);
    } // else ok, not all tasks will have replicas
}

static void wq_spec_submit_replicas(wq_speculative_queue *wq){
    // Does not create any replicas if multiplier is negative.
    // Allows for regular Work Queue behavior.
    if (wq->multiplier < 0.0) return;
    struct work_queue_stats s;
    work_queue_get_stats(wq->q, &s);
    if (s.tasks_done > 5){
        uint64_t average = (s.time_workers_execute_good + s.time_send_good + s.time_receive_good) / s.tasks_done;
        if (average>0){
            uint64_t now = timestamp_get();
            uint64_t spec_trigger = average*wq->multiplier;
            debug(D_WQ, "Current speculative trigger is %f secs.", spec_trigger*1.e-6);
            for (auto& p: wq->tasks){
                struct work_queue_task* t1 = p.second;
                // Process tasks that are running and do not have replicas already
                if (work_queue_task_state(wq->q, t1->taskid) == WORK_QUEUE_TASK_RUNNING &&
                    wq->replicas.count(t1->taskid)==0) {
                  uint64_t runtime = now - t1->time_when_commit_start;
                  int priority = t1->priority + wq->priority_change;
                  if (runtime >= spec_trigger) {
                    struct work_queue_task *bkp = work_queue_task_clone(t1);
                    work_queue_task_specify_category(bkp, "replica");
                    work_queue_task_specify_priority(bkp, priority);
                    int replica = work_queue_submit(wq->q, bkp);
                    debug(D_WQ,
                          "Task %d is taking too long (%f of %f avg), "
                          "submitting speculative task %d.",
                          t1->taskid, runtime * 1e-6, average * 1e-6, replica);
                    // Add two mappings, one from the original to the clone
                    wq->replicas[t1->taskid] = replica;
                    // And from the clone to the original.
                    wq->replicas[replica] = t1->taskid;
                  }
                }
            }
        }
    }
}
work_queue_task *wq_wait(wq_speculative_queue* wq, int timeout){
    wq_spec_submit_replicas(wq);
    struct work_queue_task* t = work_queue_wait(wq->q, timeout);
    if(t && t->result == WORK_QUEUE_RESULT_SUCCESS){
        wq_spec_check_replica(wq, t);
        wq->tasks.erase(t->taskid);
    }
    return t;
}

void wq_delete(wq_speculative_queue* wq){
    work_queue_delete(wq->q);
    delete wq;
}

struct work_queue* wq_q(wq_speculative_queue *wq){
    return wq->q;
}
