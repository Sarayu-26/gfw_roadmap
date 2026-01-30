#!/bin/bash
#SBATCH --job-name=birdlife_gfw_FF
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/birdlife_gfw_FF_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/birdlife_gfw_FF_%j.err
#SBATCH --chdir=/home/sandbox-sparc/gfw_roadmap

set -euo pipefail

echo "[SLURM] Host: $(hostname)"
echo "[SLURM] Cores allocated: ${SLURM_CPUS_PER_TASK}"
echo "[SLURM] Job started at: $(date)"

# --- CRITICAL: disable renv autoloader (avoids KernSmooth / BLAS issues)
export RENV_CONFIG_AUTOLOADER_ENABLED=FALSE

# Ensure future / BLAS respect Slurm allocation
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}

# Run the BirdLife R workflow (absolute path to avoid ambiguity)
Rscript /home/sandbox-sparc/gfw_roadmap/scripts/05_run_birdlife_fishing_effort.R

echo "[SLURM] Job finished at: $(date)"