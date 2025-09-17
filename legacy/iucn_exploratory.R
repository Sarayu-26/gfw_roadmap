library(terra)
library(sf)
library(dplyr)

iucn_v01 <- readRDS("data/iucn_marine_clean_v02.rds")
# ex <- iucn_v01[iucn_v01$binomial == "Morus capensis",]

sps <- c(
  "Eubalaena glacialis",
  "Eschrichtius robustus",
  "Balaenoptera edeni",
  "Balaenoptera physalus",
  "Balaenoptera bonaerensis",
  "Balaenoptera acutorostrata",
  "Orcinus orca",
  "Sousa chinensis",
  "Pseudorca crassidens",
  "Sousa teuszii",
  "Cephalorhynchus hectori",
  "Orcaella brevirostris",
  "Sousa plumbea",
  "Cephalorhynchus commersonii",
  "Delphinus delphis",
  "Feresa attenuata",
  "Globicephala macrorhynchus",
  "Globicephala melas",
  "Grampus griseus",
  "Lagenodelphis hosei",
  "Lagenorhynchus acutus",
  "Lagenorhynchus albirostris",
  "Lagenorhynchus australis",
  "Lagenorhynchus obliquidens",
  "Lagenorhynchus obscurus",
  "Lissodelphis borealis",
  "Lissodelphis peronii",
  "Peponocephala electra",
  "Stenella attenuata",
  "Stenella clymene",
  "Stenella coeruleoalba",
  "Stenella frontalis",
  "Stenella longirostris",
  "Steno bredanensis",
  "Tursiops truncatus",
  "Cephalorhynchus eutropia",
  "Cephalorhynchus heavisidii",
  "Sotalia guianensis",
  "Tursiops aduncus",
  "Orcaella heinsohni",
  "Sousa sahulensis",
  "Kogia breviceps",
  "Kogia sima",
  "Phocoena sinus",
  "Neophocaena asiaeorientalis",
  "Phocoena phocoena",
  "Neophocaena phocaenooides",
  "Physeter macrocephalus",
  "Pontoporia blainvillei",
  "Mesoplodon perrini",
  "Berardius arnuxii",
  "Berardius bairdii",
  "Hyperoodon planifrons",
  "Indopacetus pacificus",
  "Mesoplodon bidens",
  "Mesoplodon europaeus",
  "Mesoplodon grayi",
  "Mesoplodon layardii",
  "Mesoplodon mirus",
  "Mesoplodon peruvianus",
  "Mesoplodon densirostris",
  "Ziphius cavirostris",
  "Berardius minimus",
  "Hyperoodon ampullatus",
  "Mesoplodon stejnegeri",
  "Rhincodon typus",
  "Sphyrna lewini",
  "Sphyrna zygaena",
  "Sphyrna mokarran",
  "Carcharhinus falciformis",
  "Galeocerdo cuvier",
  "Carcharhinus brachyurus",
  "Carcharhinus galapagensis",
  "Carcharhinus plumbeus",
  "Cetorhinus maximus",
  "Carcharodon carcharias",
  "Somniosus pacificus",
  "Somniosus microcephalus",
  "Odontaspis noronhai",
  "Pseudocarcharias kamoharai",
  "Megachasma pelagios",
  "Alopias vulpinus",
  "Alopias pelagicus",
  "Alopias superciliosus",
  "Isurus oxyrinchus",
  "Isurus paucus",
  "Lamna ditropis",
  "Lamna nasus",
  "Carcharhinus longimanus",
  "Prionace glauca",
  "Carcharhinus leucas",
  "Glyphis glyphis",
  "Notorynchus cepedianus",
  "Mobula birostris",
  "Mobula mobular",
  "Mobula tarapacana",
  "Mobula thurstoni",
  "Mobula kuhlii",
  "Mobula eregoodoo",
  "Mobula hypostoma",
  "Mobula munkiana",
  "Aetomylaeus vespertilio",
  "Myliobatis aquila",
  "Megatrygon microps",
  "Urogymnus polylepis",
  "Pteroplatytrygon violacea",
  "Gymnura altavela",
  "Urolophus bucculentus",
  "Aetobatus narinari",
  "Mola Mola",
  "Thunnus orientalis",
  "Dermochelys coriacea",
  "Chelonia mydas",
  "Eretmochelys imbricata",
  "Caretta caretta",
  "Natator depressus",
  "Lepidochelys olivacea",
  "Lepidochelys kempii",
  "Pelecanus crispus",
  "Diomedea exulans",
  "Diomedea epomophora",
  "Diomedea dabbenena",
  "Diomedea amsterdamensis",
  "Diomedea antipodensis",
  "Diomedea sanfordi",
  "Phoebastria irrorata",
  "Phoebastria albatrus",
  "Phoebastria nigripes",
  "Phoebastria immutabilis",
  "Thalassarche melanophris",
  "Thalassarche impavida",
  "Thalassarche cauta",
  "Thalassarche eremita",
  "Thalassarche salvini",
  "Thalassarche chrysostoma",
  "Thalassarche chlororhynchos",
  "Thalassarche carteri",
  "Thalassarche bulleri",
  "Phoebetria fusca",
  "Phoebetria palpebrata",
  "Papasula abbotti",
  "Morus bassanus",
  "Morus capensis",
  "Morus serrator",
  "Pterodroma solandri",
  "Pseudobulweria macgillivrayi",
  "Procellaria aequinoctialis",
  "Procellaria conspicillata",
  "Procellaria parkinsoni",
  "Procellaria westlandica",
  "Ardenna gravis",
  "Calonectris diomedea",
  "Ardenna tenuirostris",
  "Puffinus puffinus",
  "Ardenna grisea",
  "Larus argentatus",
  "Larus audouinii",
  "Alca torda",
  "Fratercula arctica",
  "Uria lomvia",
  "Somateria mollissima",
  "Fregata aquila",
  "Pygoscelis papua",
  "Eudyptes moseleyi",
  "Eudyptes pachyrhynchus",
  "Eudyptes robustus",
  "Megadyptes antipodes",
  "Eudyptula minor",
  "Spheniscus humboldti",
  "Spheniscus magellanicus",
  "Spheniscus mendiculus",
  "Arctocephalus australis",
  "Arctocephalus forsteri",
  "Arctocephalus gazella",
  "Arctocephalus galapagoensis",
  "Arctocephalus philippii",
  "Arctocephalus pusillus",
  "Arctocephalus townsendi",
  "Arctocephalus tropicalis",
  "Callorhinus ursinus",
  "Eumetopias jubatus",
  "Neophoca cinerea",
  "Otaria byronia",
  "Phocarctos hookeri",
  "Cystophora cristata",
  "Erignathus barbatus",
  "Halichoerus grypus",
  "Histriophoca fasciata",
  "Mirounga angustirostris",
  "Monachus monachus",
  "Neomonachus schauinslandi",
  "Pusa hispida",
  "Odobenus rosmarus")

# length(sps)
#==============================================================================
# This is an example to filter what species in our roadmap list are
#present in the iucn_v01 data frame
ex <- iucn_v01 %>%
  dplyr::filter(binomial %in% sps)
unique(ex$binomial)
# 
#find species in our list 'sps' are NOT prsent in the subset 'ex'
sps[!sps %in% ex$binomial]

#count how many species are missing and add the total number in 'sps' 
length(sps[!sps %in% ex$binomial]) + length(sps)

#==============================================================================
# This is the code sequence to run the loop to save individual species files
#we already did this!! Don't need to do again
for(i in seq_along(sps)) {
  single <- iucn_v01[iucn_v01$binomial == sps[i],]
  if(nrow(single) > 0){
    # final <- single
    nsm <- unique(single$binomial)
    opath <- paste0("outputs/iucn_sps/", nsm, ".rds")
    saveRDS(single, opath)
  }
}
#==============================================================================
#Return exact species in roadmap list that are NOT present in iucn_v01
#Opening script and loaded dplyr, read RDS file to recreate iucn_v01 &
#copied sps vector from above
missing <- sps[!sps %in% iucn_v01$binomial]
missing
length(missing)
writeLines(missing,"data/roadmap_species_missing.txt")
#==============================================================================
#double checking all the species are accounted for!! 
species_check <-data.frame(
  species = sps, 
  present_in_dataset = sps %in% iucn_v01$binomial
)

#reorder all present first (true) then all missing (false)
species_check <-species_check[order(-species_check$present_in_dataset), ]

#out of 193 species on our roadmap list we have 109 in IUCN dataset and 84 missing 
#table checks out compared to previous list

#i don't know triple checking comparing to roadmap missing file
#yay it says true character (0) no species missing it matches
roadmap_missing <- readLines("data/roadmap_species_missing.txt")
false_species <- species_check$species[!species_check$present_in_dataset]
identical(sort(false_species), sort(roadmap_missing))
# In species_check but not in roadmap file
setdiff(false_species, roadmap_missing)
# In roadmap file but not in species_check
setdiff(roadmap_missing, false_species)
#==============================================================================
