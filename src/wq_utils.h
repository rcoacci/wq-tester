
#ifndef WQ_UTILS_H_
#define WQ_UTILS_H_

extern "C" {
#include "cctools/work_queue.h"
}

#include <unordered_map>

int wq_submit_clone(struct work_queue* q, struct work_queue_task* t, std::unordered_map<int, int>& backup_tasks, int backup_prio);

struct work_queue_task *wq_wait_clone(struct work_queue *q, int timeout,std::unordered_map<int,int>& backup_tasks);

int wq_submit_speculative(struct work_queue* q, struct work_queue_task* t, std::unordered_map<int, work_queue_task*>& tasks);

struct work_queue_task *wq_wait_speculative(struct work_queue *q, int timeout, double spec_mult,
                                            std::unordered_map<int,int>& replicas,
                                            std::unordered_map<int, work_queue_task*>& tasks);
#endif // WQ_UTILS_H_
