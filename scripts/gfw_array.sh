#!/bin/bash
#SBATCH --job-name=gfw005
#SBATCH --partition=long
#SBATCH --time=0
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --array=1-16
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/gfw_%A_%a.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/gfw_%A_%a.err

# ---- Paths ----
BASE_DIR="/home/sandbox-sparc/gfw_roadmap"
SCRIPT="${BASE_DIR}/scripts/gfw_txt_to_rds_005_onefile.R"
LOG_DIR="${BASE_DIR}/logs"

mkdir -p "${LOG_DIR}"

# ---- Move to project directory (important for relative paths in R) ----
cd "${BASE_DIR}" || exit 1

# ---- Run ----
Rscript "${SCRIPT}" "${SLURM_ARRAY_TASK_ID}"