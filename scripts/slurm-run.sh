#!/bin/bash
#SBATCH -J "wq-tester"
#SBATCH --partition=SEDE
#SBATCH --mail-type=NONE
#SBATCH -o "%j-Dump.log"
#SBATCH --exclusive

export PATH="$PATH:/tgdesenv/src/proj0/up21/cctools/bin"
WORKDIR="$SLURM_JOBID-workdir"

mkdir -p $WORKDIR
cd $WORKDIR 
ln -s ../input.0 input.0
cp -L ../wq-tester ../wq-work ./
sleep 1
srun -o "Dump-manager.log" --overlap -N1 -n1 ./wq-tester $@ &
sleep 1
srun -o "Dump-%3t.log" --overlap work_queue_worker -d wq --cores 1 -s /scr01 -t 3600 $(hostname) 9123 &
wait
