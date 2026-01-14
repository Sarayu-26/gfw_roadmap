#!/usr/bin/env bash
set -euo pipefail

IN_GPKG="/Users/ibrito/Downloads/species/BOTW_2025.gpkg"
OUT_GPKG="/Users/ibrito/Downloads/species/BOTW_2025_subset_52.gpkg"
LAYER="all_species"

# Remove output if it already exists (avoid ogr2ogr appending)
rm -f "$OUT_GPKG"

ogr2ogr \
-f GPKG "$OUT_GPKG" "$IN_GPKG" \
-nln "$LAYER" \
-where "sci_name IN (
    'Pelecanus crispus',
    'Diomedea exulans',
    'Diomedea epomophora',
    'Diomedea dabbenena',
    'Diomedea amsterdamensis',
    'Diomedea antipodensis',
    'Diomedea sanfordi',
    'Phoebastria irrorata',
    'Phoebastria albatrus',
    'Phoebastria nigripes',
    'Phoebastria immutabilis',
    'Thalassarche melanophris',
    'Thalassarche impavida',
    'Thalassarche cauta',
    'Thalassarche eremita',
    'Thalassarche salvini',
    'Thalassarche chrysostoma',
    'Thalassarche chlororhynchos',
    'Thalassarche carteri',
    'Thalassarche bulleri',
    'Phoebetria fusca',
    'Phoebetria palpebrata',
    'Morus bassanus',
    'Morus capensis',
    'Morus serrator',
    'Pterodroma solandri',
    'Pseudobulweria macgillivrayi',
    'Procellaria aequinoctialis',
    'Procellaria conspicillata',
    'Procellaria parkinsoni',
    'Procellaria westlandica',
    'Ardenna gravis',
    'Calonectris diomedea',
    'Ardenna tenuirostris',
    'Puffinus puffinus',
    'Ardenna grisea',
    'Larus argentatus',
    'Larus audouinii',
    'Alca torda',
    'Fratercula arctica',
    'Uria lomvia',
    'Somateria mollissima',
    'Fregata aquila',
    'Pygoscelis papua',
    'Eudyptes moseleyi',
    'Eudyptes pachyrhynchus',
    'Eudyptes robustus',
    'Megadyptes antipodes',
    'Eudyptula minor',
    'Spheniscus humboldti',
    'Spheniscus magellanicus',
    'Spheniscus mendiculus'
  )"

echo "Wrote: $OUT_GPKG"

# Quick sanity check (lists layers and feature count)
ogrinfo -so "$OUT_GPKG" "$LAYER" | head -n 40