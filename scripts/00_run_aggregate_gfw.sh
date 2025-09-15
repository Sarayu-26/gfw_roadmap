#!/bin/bash
#SBATCH --job-name=agg_gfw
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=15
#SBATCH --time=04:00:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --output=/home/sandbox-sparc/gfw-fronts-megafauna/logs/agg_gfw_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw-fronts-megafauna/logs/agg_gfw_%j.err
#SBATCH --chdir=/home/sandbox-sparc/gfw-fronts-megafauna

set -euo pipefail

echo "[SLURM] Host: $(hostname)"
echo "[SLURM] Cores allocated: ${SLURM_CPUS_PER_TASK}"

# --- Stage Parquet file to node-local scratch (faster I/O) ---
SRC="/home/sandbox-sparc/gfw-fronts-megafauna/data-raw/gfw_data_by_flag_and_gear_v20250820.parquet"
LOCAL="${SLURM_TMPDIR:-/tmp}/gfw_data_by_flag_and_gear_v20250820.parquet"

mkdir -p "$(dirname "$LOCAL")"
cp -f "$SRC" "$LOCAL"
echo "[SLURM] Using local parquet: $LOCAL"

# --- Run R script explicitly with environment variables ---
PARQUET_PATH="$LOCAL" \
ARROW_NUM_THREADS="${SLURM_CPUS_PER_TASK}" \
Rscript scripts/00_run_aggregate_gfw.R