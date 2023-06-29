#!/usr/bin/env sh

CCTOOLS_PATH=/home/rcoacci/mestrado/conda
export PATH="$PATH:$CCTOOLS_PATH/bin"
WORKDIR="workdir-XXXXXX"

WORKDIR=$(mktemp -d "$WORKDIR")
echo "Running on ${WORKDIR}"
cd $WORKDIR
for file in input{0..9}; do
    ln -s ../$file $file
done
sleep 1
../wq-tester $@ > "Dump-manager.log" &
sleep 1
parallel 'work_queue_worker -d wq --cores 1 -s $(pwd) -t 3600 $(hostname) 9123 &> Dump_worker-{}.log' ::: {00..09}
wait
