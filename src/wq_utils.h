
#ifndef WQ_UTILS_H_
#define WQ_UTILS_H_

extern "C" {
#include "cctools/work_queue.h"
}


struct wq_speculative_queue;

wq_speculative_queue* wq_create(int port, double spec_mult);

struct work_queue* wq_q(wq_speculative_queue *wq);

void wq_specify_priority_change(wq_speculative_queue* wq, int change);

void wq_specify_fast_abort(wq_speculative_queue *wq, double multiplier);

int wq_submit(wq_speculative_queue* q, work_queue_task* t);

work_queue_task *wq_wait(wq_speculative_queue* q, int timeout);

void wq_delete(wq_speculative_queue* wq);

#endif // WQ_UTILS_H_
