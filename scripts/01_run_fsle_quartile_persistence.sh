#!/bin/bash
#SBATCH --job-name=fsle_quartiles
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --output=/home/sandbox-sparc/gfw-fronts-megafauna/logs/fsle_quartiles_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw-fronts-megafauna/logs/fsle_quartiles_%j.err
#SBATCH --chdir=/home/sandbox-sparc/gfw-fronts-megafauna

set -euo pipefail

echo "[SLURM] Host: $(hostname)"
echo "[SLURM] Cores allocated: ${SLURM_CPUS_PER_TASK}"
echo "[SLURM] Job started at: $(date)"

# Run the R script (absolute path, since .sh lives elsewhere)
Rscript /home/sandbox-sparc/gfw-fronts-megafauna/scripts/01_run_fsle_quartile_persistence.R

echo "[SLURM] Job finished at: $(date)"