#!/bin/bash
#SBATCH --job-name=agg_gfw
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ibrito@eri.ucsb.edu
#SBATCH --output=/home/sandbox-sparc/gfw_roadmap/logs/agg_gfw_%j.out
#SBATCH --error=/home/sandbox-sparc/gfw_roadmap/logs/agg_gfw_%j.err
#SBATCH --chdir=/home/sandbox-sparc/gfw_roadmap

Rscript /home/sandbox-sparc/gfw_roadmap/scripts/agg_gfw_txt_to_rds_005deg.R