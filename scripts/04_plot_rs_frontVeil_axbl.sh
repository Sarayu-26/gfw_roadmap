#!/bin/bash
#SBATCH -p grit_nodes
#SBATCH --job-name=gfw_plot_veil
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5
#SBATCH --mem=128G
#SBATCH -t 3-00:00:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --chdir=/home/sandbox-sparc/gfw_roadmap
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/plot_veil_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/plot_veil_%j.err

###############################################################################
# 04_plot_rs_frontVeil_axbl.sh
#
# Author:
#   - Isaac Brito-Morales
#
# Purpose:
#   - Activate renv environment on compute node.
#   - Run global species-at-threat raster plotting with FSLE veil overlay.
#   - Offload expensive raster -> dataframe conversion + ggplot rendering to GRIT.
#
# Key ideas:
#   - CPU demand mainly from terra::resample() and raster → dataframe.
#   - Memory pressure comes from ggplot tile rendering.
#   - No package installation allowed in batch jobs (HPC-safe).
###############################################################################

set -euo pipefail

echo "Running on: $(hostname)"
echo "Working dir: $(pwd)"
echo "Job started at: $(date)"
echo

# Activate renv explicitly
Rscript -e 'if (requireNamespace("renv", quietly=TRUE)) renv::activate()'

# Run plotting script
Rscript scripts/04_plot_rs_frontVeil_axbl.R

echo
echo "Job finished at: $(date)"