#!/bin/bash
#SBATCH -p grit_nodes
#SBATCH -J count_species_at_threat_gfw
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --mem=128G
#SBATCH -t 08:00:00
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/count_species_at_threat_gfw_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/count_species_at_threat_gfw_%j.err
#SBATCH --chdir=/home/sandbox-sparc/gfw_roadmap

set -euo pipefail

echo "[SLURM] Host: $(hostname)"
echo "[SLURM] Cores allocated: ${SLURM_CPUS_PER_TASK}"
echo "[SLURM] Job started at: $(date)"

# Disable renv autoloader (consistent with your other jobs)
export RENV_CONFIG_AUTOLOADER_ENABLED=FALSE

# Make sure BLAS / OpenMP respect Slurm allocation
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}

# Ensure logs dir exists
mkdir -p /home/sandbox-sparc/gfw_roadmap/logs

# Run the R script
Rscript /home/sandbox-sparc/gfw_roadmap/R/count_species_at_threat_gfw.R

echo "[SLURM] Job finished at: $(date)"