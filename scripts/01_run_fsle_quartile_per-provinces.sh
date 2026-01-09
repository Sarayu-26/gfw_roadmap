#!/bin/bash
#SBATCH --job-name=fsle_quartiles
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/fsle_quartiles_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/fsle_quartiles_%j.err
#SBATCH --chdir=/home/sandbox-sparc/gfw_roadmap

set -euo pipefail

echo "[SLURM] Host: $(hostname)"
echo "[SLURM] Cores allocated: ${SLURM_CPUS_PER_TASK}"
echo "[SLURM] Job started at: $(date)"

# --- CRITICAL: disable renv autoloader (this was causing the KernSmooth error)
export RENV_CONFIG_AUTOLOADER_ENABLED=FALSE

# Run the R script (absolute path to avoid ambiguity)
Rscript /home/sandbox-sparc/gfw_roadmap/scripts/01_run_fsle_quartile_per-provinces.R

echo "[SLURM] Job finished at: $(date)"