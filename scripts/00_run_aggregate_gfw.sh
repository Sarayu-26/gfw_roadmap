#!/bin/bash
#SBATCH --job-name=agg_gfw
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5
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

# Copy parquet to local scratch (faster I/O)
mkdir -p "$(dirname "$LOCAL")"
cp -f "$SRC" "$LOCAL"
echo "[SLURM] Using local parquet: $LOCAL"

# --- Concurrency controls ---
export GEAR_WORKERS="${SLURM_CPUS_PER_TASK}"  # number of gear workers
export ARROW_NUM_THREADS=1                    # keep Arrow lean per worker
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

# --- Run R aggregation (writes per-gear TSV shards) ---
PARQUET_PATH="$LOCAL" Rscript scripts/00_run_aggregate_gfw.R

# --- Make one big TXT per gear (merge that gear's shards only) ---
for d in outputs/agg_cell_*_tsv; do
  [ -d "$d" ] || continue
  g="$(basename "$d" | sed -E 's/^agg_cell_(.*)_tsv$/\1/')"
  awk 'FNR==1 && NR!=1 {next} {print}' "$d"/*.csv > "outputs/agg_cell_${g}_full.txt"
  echo "[SLURM] Wrote outputs/agg_cell_${g}_full.txt"
done