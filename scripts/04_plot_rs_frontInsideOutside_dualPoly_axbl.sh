#!/bin/bash
#SBATCH -p grit_nodes
#SBATCH --job-name=plot_insideOutsideDualPoly
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --mem=128G
#SBATCH -t 02:00:00
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --chdir=/home/sandbox-sparc/gfw_roadmap
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/plot_insideOutsideDualPoly_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/plot_insideOutsideDualPoly_%j.err

###############################################################################
# 04_plot_rs_frontInsideOutside_dualPoly_axbl.sh
#
# Author:
#   - Isaac Brito-Morales
#
# Purpose:
#   - Run global species-at-threat raster plotting using:
#       * full color scale inside the union of FSLE and thermal polygons
#       * grey scale outside those polygons
#   - Overlay both polygon sets separately in the final figure.
#
# Key ideas:
#   - CPU demand mainly from raster to dataframe conversion and point-in-polygon
#     classification.
#   - Memory pressure comes from global ggplot tile rendering.
#   - renv activation is handled inside the R script.
#   - Final figure is written to scratch:
#       /home/hpc-scratch/ibrito/
###############################################################################

set -euo pipefail

echo "Running on: $(hostname)"
echo "Working dir: $(pwd)"
echo "Job started at: $(date)"
echo

Rscript scripts/04_plot_rs_frontInsideOutside_dualPoly_axbl.R

echo
echo "Job finished at: $(date)"