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

# Copy parquet to node-local scratch (faster I/O)
mkdir -p "$(dirname "$LOCAL")"
cp -f "$SRC" "$LOCAL"
echo "[SLURM] Using local parquet: $LOCAL"

# --- Concurrency controls ---
export GEAR_WORKERS=1                      # run one gear at a time (was ${SLURM_CPUS_PER_TASK})
export ARROW_NUM_THREADS=1                 # Arrow threads per worker (keep lean)
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

# --- BLAS shim: ensure libRblas.so is resolvable on this node ---
R_HOME="$(R RHOME 2>/dev/null || true)"
if [ -n "${R_HOME}" ] && [ -f "$R_HOME/lib/libRblas.so" ]; then
  export LD_LIBRARY_PATH="$R_HOME/lib:${LD_LIBRARY_PATH:-}"
  echo "[SLURM] Using R BLAS at $R_HOME/lib/libRblas.so"
else
  mkdir -p "$HOME/lib"
  if   [ -f /usr/lib64/libblas.so ];   then ln -sf /usr/lib64/libblas.so   "$HOME/lib/libRblas.so"
  elif [ -f /usr/lib64/libblas.so.3 ]; then ln -sf /usr/lib64/libblas.so.3 "$HOME/lib/libRblas.so"
  fi
  export LD_LIBRARY_PATH="$HOME/lib:${LD_LIBRARY_PATH:-}"
  echo "[SLURM] Using system BLAS via \$HOME/lib/libRblas.so"
fi

# --- Run R aggregation (writes per-gear TSV shards) ---
PARQUET_PATH="$LOCAL" Rscript scripts/00_run_aggregate_gfw.R

# --- Make one big TXT per gear (merge that gear's shards only) ---
for d in outputs/agg_cell_*_tsv; do
  [ -d "$d" ] || continue
  g="$(basename "$d" | sed -E 's/^agg_cell_(.*)_tsv$/\1/')"
  awk 'FNR==1 && NR!=1 {next} {print}' "$d"/*.csv > "outputs/agg_cell_${g}_full.txt"
  echo "[SLURM] Wrote outputs/agg_cell_${g}_full.txt"
done