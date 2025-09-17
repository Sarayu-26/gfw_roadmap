#!/bin/bash
#SBATCH --job-name=agg_gfw
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=6          # adjust up/down depending on load
#SBATCH --time=04:00:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/agg_gfw_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/agg_gfw_%j.err
#SBATCH --chdir=/home/sandbox-sparc/gfw_roadmap

set -euo pipefail

echo "[SLURM] Host: $(hostname)"
echo "[SLURM] Cores allocated: ${SLURM_CPUS_PER_TASK}"
echo "[SLURM] Tmp dir: ${SLURM_TMPDIR:-/tmp}"

# --- Source and target paths ---
SRC="/home/sandbox-sparc/gfw_roadmap/data/gfw_data_by_flag_and_gear_v20250820.parquet"
LOCAL="${SLURM_TMPDIR:-/tmp}/gfw_data_by_flag_and_gear_v20250820.parquet"

# --- Ensure scratch exists & has space ---
mkdir -p "$(dirname "$LOCAL")"
src_size=$(du -m "$SRC" | awk '{print $1}')
free_tmp=$(df -m "$(dirname "$LOCAL")" | awk 'NR==2{print $4}')
echo "[SLURM] Source size: ${src_size} MB | Scratch free: ${free_tmp} MB"
if [ "$free_tmp" -le "$src_size" ]; then
  echo "[ERROR] Not enough scratch space to copy parquet. Exiting." >&2
  exit 1
fi

# --- Copy parquet to local scratch (faster I/O) ---
cp -f "$SRC" "$LOCAL"
if [ ! -s "$LOCAL" ]; then
  echo "[ERROR] Copy to scratch failed: $LOCAL" >&2
  exit 1
fi
echo "[SLURM] Using local parquet: $LOCAL"
du -h "$LOCAL" || true

# Optional: clean up scratch on exit
cleanup() { rm -f "$LOCAL" || true; }
trap cleanup EXIT

# --- Tame threading to reduce RAM pressure ---
export ARROW_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

# --- Run ---
PARQUET_PATH="$LOCAL" Rscript scripts/00_run_aggregate_gfw.R