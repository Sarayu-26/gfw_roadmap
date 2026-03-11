#!/bin/bash
#SBATCH -p grit_nodes
#SBATCH --job-name=plot_frontAlphaCont
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --mem=128G
#SBATCH -t 02:00:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --chdir=/home/sandbox-sparc/gfw_roadmap
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/plot_frontAlphaCont_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/plot_frontAlphaCont_%j.err

###############################################################################
# 04_plot_rs_frontAlpha_axbl_contPoly.sh
#
# Author:
#   - Isaac Brito-Morales
#
# Purpose:
#   - Run global species-at-threat raster plotting with front-based variable
#     alpha and front polygon delineation.
#   - Uses continuous species-at-threat values.
#
# Key ideas:
#   - CPU demand mainly from terra::resample() and raster -> dataframe.
#   - Memory pressure comes from ggplot tile rendering.
#   - renv activation is handled inside the R script.
###############################################################################

set -euo pipefail

echo "Running on: $(hostname)"
echo "Working dir: $(pwd)"
echo "Job started at: $(date)"
echo

# Run plotting script
Rscript scripts/04_plot_rs_frontAlpha_axbl_contPoly.R

echo
echo "Job finished at: $(date)"