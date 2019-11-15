try(source("R/shrimp_support_tables.R"))

## Import area definitions
reitmapping_fjords <-
  tbl_mar(mar,'fiskar.skikar') %>%
  #weeds out Fleming cap, other úthaf locations
  filter(hafsvaedi == "GRUNNSLÓÐ") %>% 
  #original gridcell definitions are removed because it results in multiple lat/long and gridcell entries
  #per skiki
  mutate(#gridcell = 10*reitur + nvl(smareitur,1), 
    fjardarreitur = nvl(fj_reitur,0)) %>%  
  select(skiki, fjardarreitur, skikaheiti) %>% 
  distinct() %>% 
  collect() %>% 
  full_join(corrected_fjardarreitur %>% 
              rename(fjardarreitur=fjardarreitur.fx) %>% 
              select( -c(tognumer)) %>% 
              distinct()
  ) %>% 
  #mar:::sr2d() %>%
  #rename(gridcell = sr) %>%
  #Instead, the new gridcell naming convention, for fjords is here.
  mutate(DIVISION = skiki, SUBDIVISION = paste(skiki, fjardarreitur, sep = '_'), GRIDCELL = paste(skiki, fjardarreitur, sep = '_')) %>%
  full_join(flatarmal) %>% 
  arrange(skiki, fjardarreitur) %>% 
  mutate(id = max(reitmapping$id) + 1:n())


reitmapping <-
  reitmapping %>%
  transmute(GRIDCELL = as.character(GRIDCELL), DIVISION, SUBDIVISION = as.character(SUBDIVISION), id, lat, lon, size) %>% 
  full_join(transmute(reitmapping_fjords, GRIDCELL, DIVISION, SUBDIVISION, id, size)) 

dbWriteTable(mar,'reitmapping',as.data.frame(reitmapping),overwrite=TRUE)


## import the area definitions into mfdb
mfdb_import_area(mdb, 
                 transmute(reitmapping_fjords, GRIDCELL, DIVISION, SUBDIVISION, id, size) %>% 
                   select(id,
                          name=GRIDCELL,
                          size) %>% 
                   as.data.frame()) 

## create area division (used for e.g. bootstrapping)
mfdb_import_division(mdb,
                     plyr::dlply(transmute(reitmapping_fjords, GRIDCELL, DIVISION, SUBDIVISION, id, size),
                                 'SUBDIVISION',
                                 function(x) x$GRIDCELL))

## here some work on temperature would be nice to have instead of fixing the temperature
mfdb_import_temperature(mdb, 
                        expand.grid(year=1960:2020,
                                    month=1:12,
                                    areacell = reitmapping_fjords$GRIDCELL,
                                    temperature = 3))


## species mapping
data.frame(tegund =  c(1:11, 19, 21, 22,
                       23, 25, 27, 30, 31, 48, 14, 24, 41),
           species = c('COD','HAD','POK','WHG','RED','LIN','BLI','USK',
                       'CAA','RNG','REB','GSS','HAL','GLH',
                       'PLE','WIT','DAB','HER','CAP','LUM','MON','LEM','SHR')) %>% 
dbWriteTable(mar,'species_key',.,overwrite = TRUE)


#distinguish gears for shrimp so that data can be subsetted later to distinguish selectivity
mapping <-
  mapping %>% 
  mutate(gear = ifelse(veidarfaeri==30, 'TMS', gear))

dbWriteTable(conn = mar, 
             value = mapping,
             name = 'gear_mapping',
             overwrite = TRUE)

## Set-up sampling types

mfdb_import_sampling_type(mdb, data.frame(
  id = 18:21,
  name = c('OFS','XINS','XS','HYD'),
  description = c('Offshore shrimp survey',
                  'Extra-survey inshore shrimp samples', 'Extra samples',
                  'Ocean or hydrographic data')))

#labels observer (1/2/8) synis_id that both contain shrimp and have a skiki
#removed the requirement to have shrimp
stations_shr.1 <-
  lesa_stodvar(mar) %>% 
  left_join(tbl(mar,'skiki_areas') %>%
              select(synis_id, in.arn, in.isa, corrected_areacell)) %>%
  left_join(tbl(mar,'vessel_map')) %>% 
  #left_join(tbl(mar,'catch_map')) %>% 
  mutate(saga_nr = nvl(saga_nr,0),
         ar = to_number(to_char(dags,'yyyy')),
         man = to_number(to_char(dags,'mm'))) %>% 
  shrimp_station_fixes() %>%
  filter(synaflokkur %in% c(1,2,8,14,10,20,31,37)) %>%  
  mutate(sampling_type = 
           ifelse(synaflokkur %in% c(1,2,8),'SEA',
                  #INS is Stofnmæling innfjarðarrækju, XINS are 37s that are extra surveys.
                  ifelse(synaflokkur==37,'INS',
                         #14 renamed from 37, tows not part of survey design
                         ifelse(synaflokkur==14,'XINS',
                                #Nytjastofnarannsóknir - leiguskip is 20
                                ifelse(synaflokkur %in% c(10,20),'XS','OFS')))),
                  #OFS is Stofnmæling úthafsrækju
                  institute = 'MRI',
                  vessel = concat(concat(skip,'-'),saga_nr)) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>% 
  #below converts areacell to skiki format when:
  #   - lat and lon show it to be within skikis 52 or 53, OR
  #   - skiki not registered as 52 or 53, but sampling types are INS/XINS/XS or synis_id are SEA_fjords.
  #   - likely this means there are offshore samples in INS/XINS/XS that are labelled incorrectly with skiki based areacell
  #   - also possibly there are samples labelled as 52,53 with locations outside these fjords that have reitur based areacell
  left_join(SEA_fjords) %>% 
  mutate(areacell=ifelse(in.arn==1 | in.isa==1, concat(concat(skiki,'_'),fjardarreitur), as.character(10*reitur+nvl(smareitur,1))),
         areacell=ifelse(!is.na(corrected_areacell), corrected_areacell, areacell),
         areacell=ifelse(in.arn==0 & 
                           in.isa==0 & 
                            is.na(corrected_areacell) & 
                              (sampling_type %in% c('INS', 'XINS', 'XS') | inSEA_fjords==1), concat(concat(skiki,'_'),fjardarreitur), areacell))  

try(dbRemoveTable(mar,'stations_shr.1'), silent=T)
stations_shr.1 %>% 
  compute(name='stations_shr.1',temporary=FALSE,indexes = list('synis_id'))

stations_shr <-
  tbl(mar, 'stations_shr.1') %>% 
  #redefining sampling_type to include extra tows with no tognumer in Ísafjörðurdjúp
  mutate(tognumer = ifelse(sampling_type == 'XS' &
                                  leidangur %in% local(isa.h[25:length(isa.h)]) &
                                  ar > 2011 &
                                  areacell %in% c('53_1.1', '53_1.2', '53_1.3', '53_1.4', '53_3', '53_5') &
                                  is.na(tognumer),
                                        1000, tognumer),
        sampling_type=ifelse(tognumer==1000, 'INS', sampling_type)) %>%
  select(synis_id,ar,man,lat=kastad_n_breidd,lon=kastad_v_lengd,lat1=hift_n_breidd,lon1=hift_v_lengd,
         gear,sampling_type,depth=dypi_kastad,vessel,reitur,smareitur,skiki,fjardarreitur,
         leidangur,toglengd,tognumer,areacell) %>% 
  #EVENTUALLY skiki_areas NEEDS TO BE UPDATED TO INCLUDE ALL INSHORE AREAS SO THAT AREACELL DEF IS NOT DEPENDENT ON SAMPLING TYPE. Then only those which
  #are defined as within fjords could be added, now many will repeat
  #not sure that XS sampling type should use fjardarreitur for area size definition
  mutate(towlength = arcdist(lat,lon,lat1,lon1),
         #SHRIMP specific
         season = ifelse(man %in% 1:2,1,ifelse(man %in% 3:5, 2, ifelse(man %in% 9:10, 3, 4)))) %>%
  ##why is length taken from lat / long rather than toglengd? Former method causes 26 rows of INS data to have length = 0
  distinct() %>% 
  group_by(ar, fjardarreitur, skiki, season) %>%
  mutate(towcount = ifelse(sampling_type %in% c('INS', 'XS', 'XINS', 'SEA') & !is.na(fjardarreitur) & !is.na(tognumer), n(), #these are fjardarreiturs defined by Inga's corrections (unless she excluded some combinations of fjardarreitur x tow)
                           ifelse(sampling_type %in% c('XS','XINS','SEA') & !is.na(fjardarreitur) & is.na(tognumer), 1, #these are fjardarreiturs not defined by by Inga, still grouping by them to reduce sample size, even though scale shouldn't really matter because these are not being used for indices, only catchdistributions
                                  ifelse(sampling_type %in% c('XS','XINS','SEA') & is.na(fjardarreitur), 1, NA)))) %>% #these don't have a fjardarreitur so just count as 1 haul
  ungroup() %>% 
  #FOR NOW TOWCOUNT IS REMOVED BUT MAY BE USEFUL LATER IF NEEDED TO ADJUST INDICES
  select(-c(lat1,lon1,reitur,smareitur,fjardarreitur,skiki,leidangur, season)) %>% 
  inner_join(tbl(mar,'reitmapping') %>%
               select(areacell=GRIDCELL, size),
             by='areacell') %>%
  #below differs from how towlength imported in original database
  mutate(towlength = ifelse(!is.na(toglengd), toglengd, towlength)) %>%   
  rename(tow=synis_id)  %>% 
  rename(month = man) %>%
  rename(year = ar) %>%
  rename(latitude = lat) %>%
  rename(longitude = lon) %>% 
  #Note that anti-join only removes those found within stations with same areacell
  #THERE WILL BE DUPLICATES FOR 1,2,8 WITH DIFFERENT AREACELLS
  anti_join(tbl(mar, 'stations')) %>% 
  mutate(year = ifelse(month==12, year + 1, year)) %>% #HOTFIX FOR TIMING
  distinct()

try(dbRemoveTable(mar,'stations_shr'), silent=T)
stations_shr %>% 
  compute(name='stations_shr',temporary=FALSE,indexes = list('tow'))

tbl(mar,'stations_shr') %>% 
  rename(name = tow) %>% 
  select(-c(towcount, tognumer, size)) %>% #makes it compatible with stations
  collect(n=Inf) %>% 
  as.data.frame() %>%
  mutate(name = ifelse(name == 1e5, '100000',ifelse(name == 4e5,'400000',as.character(name)))) %>%
  mfdb_import_tow_taxonomy(mdb,.)


## length distributions - replaces original table
try(dbRemoveTable(mar,'ldist'), silent=T)
lesa_lengdir(mar) %>% 
  inner_join(tbl(mar,'species_key')) %>%
  skala_med_toldum2() %>% 
  rename(tow=synis_id) %>% 
  compute(name='ldist',temporary=FALSE)

#stations_shr should contain sampling types INS, XINS, XS and any SEA samples from within fjords that now have
#skiki_based areacells (these are doubled in tow taxonomy due to first import of SEA samples that have reitur-based areacells)
#this also produces an additional column mean_wt to accomodate biomass-based indices
ldist_shrimp <- 
  tbl(mar,'ldist') %>% 
  right_join(tbl(mar,'stations_shr'), #%>% 
             #select(-towlength), #no longer removed - retained for biomass index
             by = 'tow') %>% 
  mutate(lengd = nvl(lengd,0),
         lengd = ifelse(lengd > 4 & tegund == 41, lengd/10, lengd), #fixes 2017 shrimp entered in mm
         fjoldi = nvl(fjoldi,0), 
         kyn = ifelse(kyn == 2,'F',ifelse(kyn ==1,'M','')),
         kynthroski = ifelse(kynthroski > 1,2,ifelse(kynthroski == 1,1,NA)),
         age = 0,
         #weight VALUES ARE SCALED BY TOWCOUNT, TOWLENGTH, AND SIZE OF AREA
         #these represent mean weights, so scaling is done a priori so that it is incorporated
         #when weight is multiplied by number. weight should be mean, not total, weight here because a count column is 
         #also included - that means that wehn using mfdb_sample_totalweight, it 
         #multiplies the 2 columns to generate total weight
         weight = ifelse(is.na(mean_wt) | is.na(towcount), 
                         NA, (mean_wt/ifelse(sampling_type %in% c('XS', 'XINS', 'SEA'), 1, towlength))/towcount*ifelse(sampling_type %in% c('XS', 'XINS', 'SEA'), 1, size))) %>% 
  select(-c(r,biom.r,tegund, mean_wt, towcount, size, towlength)) %>% 
  rename(sex=kyn) %>% 
  rename(maturity_stage = kynthroski) %>% 
  rename(length = lengd) %>% 
  rename(count=fjoldi) %>%
  collect(n=Inf) %>% 
  as.data.frame()


mfdb_import_vessel_taxonomy(mdb,
                            data.frame(name=c('6121-10','1140-0','165-0','208-0','230-0',
                                              '377-0','385-0','883-0'),
                                       length=NA,tonnage=NA,power=NA,full_name = 'Old unknown vessel'))



#Below needs to be a different data source because there are
#repeated synis_id that were imported first with the original database
#as having reitur_based areacell, then again as having skiki_based areacell
#here. This problem should mostly apply to non-shrimp species under 
#synaflokkur 1,2,8 - these are imported first in initdb.R then again here.
mfdb_import_survey(mdb,
                   data_source = 'iceland-ldist-infjord',
                   ldist_shrimp %>% filter(!(tow %in% c(1e5,4e5))) %>% mutate(vessel =ifelse(vessel=='-0',NA,vessel) ))


## age -- length data - ok no age but there is weight data

aldist <-
  lesa_kvarnir(mar) %>% 
  rename(tow=synis_id) %>% 
  inner_join(tbl(mar,'species_key')) %>%
  right_join(tbl(mar,'stations_shr'), #%>% 
             select(-towlength), 
             by = 'tow') %>% 
  mutate(lengd = nvl(lengd,0),
         count = 1,
         kyn = ifelse(kyn == 2,'F',ifelse(kyn ==1,'M',NA)),
         kynthroski = ifelse(kynthroski > 1,2,ifelse(kynthroski == 1,1,NA)))%>%
  select(tow, latitude,longitude, year,month, areacell, gear, vessel,
         sampling_type,count,species,
         age=aldur,sex=kyn,maturity_stage = kynthroski,
         length = lengd, no = nr, weight = oslaegt,
         gutted = slaegt, liver = lifur, gonad = kynfaeri) %>% 
  collect(n=Inf) %>% 
  as.data.frame()


mfdb_import_survey(mdb,
                   data_source = 'iceland-aldist-infjord',
                   aldist%>% 
                     filter(!(tow %in% c(1e5,4e5))) %>% 
                     mutate(vessel =ifelse(vessel=='-0',NA,vessel)))


## landings 
port2division <- function(hofn, skiki = FALSE){
  
  hafnir.numer <- rep(0,length(hofn))
  hafnir.numer[hofn<=15] <- 110
  hafnir.numer[hofn>=16 & hofn<=56] <- 101
  hafnir.numer[hofn>=57 & hofn<=81] <- 102
  hafnir.numer[hofn>=82 & hofn<=96] <- 104
  hafnir.numer[hofn==97] <- 103
  hafnir.numer[hofn>=98 & hofn<=115] <- 104
  hafnir.numer[hofn>=116 & hofn<=121] <- 105
  hafnir.numer[hofn>=122 & hofn<=148] <- 106
  hafnir.numer[hofn==149] <- 109
  hafnir.numer[hofn>=150] <- 111
  
  data <- data.frame(hofn=hofn,division=hafnir.numer) 
  
  if(skiki == TRUE){
    
    hafnir.numer[hofn %in% 57:61] <- 52
    hafnir.numer[hofn %in% 65:77] <- 53
    hafnir.numer[hofn %in% 38:56] <- 34 #represents skikis 31:34
    hafnir.numer[hofn %in% 78:87] <- 55
    hafnir.numer[hofn %in% 117:121] <- 56
    #hafnir.numer_sk[hofn %in% ??] <- 59
    hafnir.numer[hofn %in% 89:93] <- 62
    hafnir.numer[hofn %in% 115] <- 63
    
    data <- data.frame(hofn=hofn,division=hafnir.numer) 
  }
  
  return(data)
}


port2division(0:999, skiki = TRUE) %>% 
  left_join(gridcell.mapping) %>% 
  dbWriteTable(mar,'port2sk',.,overwrite=TRUE)

kfteg2division<-
  data_frame(
    division = c(31, 32, 34, 52, 53, 55, 56, 59, 62, 63),
    kfteg =    c(58, NA, NA, 51, 53, 52, 55, 72, 54, 56) 
    #landings by snaefellsness default to division 31, but actually for 32 & 34 also
  ) %>% 
  left_join(gridcell.mapping)
dbWriteTable(mar,'kfteg2sk',kfteg2division,overwrite=TRUE)


landed_catch_shrimp <- 
  lods_oslaegt(mar) %>%   
  left_join(landings_map) %>% 
  filter(ar > 1993, fteg == 41) %>% 
  select(veidarfaeri, skip_nr, fteg, kfteg,
         ar, man, hofn, magn_oslaegt, 
         veidisvaedi, l_dags, saga_nr) %>% 
  dplyr::union_all(tbl(mar,'landed_catch_pre94') %>% 
                     filter(ar < 1994, fteg == 41) %>% 
                     mutate(l_dags = to_date(concat(ar,man),'yyyymm'),
                            saga_nr = 0,
                            kfteg = NA)) %>%
  left_join(tbl_mar(mar,'kvoti.skipasaga'), by=c('skip_nr','saga_nr')) %>% 
  mutate(vessel = concat(concat(nvl(skip_nr,''),'-'),nvl(saga_nr,0)),
         flokkur = nvl(flokkur,0)) %>% 
  #select(skip_nr,hofn,l_dags,saga_nr,gerd,fteg,kfteg,veidisvaedi,stada,veidarfaeri,magn_oslaegt, i_gildi,flokkur,ar,man) %>% 
  filter(veidisvaedi == 'I',flokkur != -4) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>%
  inner_join(tbl(mar,'species_key'),by=c('fteg'='tegund')) %>%
  left_join(tbl(mar,'port2sr'),by='hofn') %>% 
  #left_join(tbl(mar,'port2sk'),by='hofn') %>% 
  left_join(tbl(mar,'kfteg2sk'),by='kfteg') %>% 
  mutate(areacell = ifelse(kfteg %in% c(58, 51, 53, 52, 55, 72, 54, 56), areacell.y, areacell.x),
         sampling_type='LND',
         gear = nvl(gear,'LLN')) %>% 
  select(weight_total=magn_oslaegt,sampling_type,areacell, vessel,species,year=ar,month=man,
         gear) %>% 
  mutate(year = ifelse(month==12, year + 1, year)) #HOTFIX FOR TIMING
  

# landed_catch_shrimp %>% 
#   group_by(species,year) %>% 
#   #summarise(catch=sum(ifelse(year>1995,count,count/0.8))/1000) %>% 
#   summarise(catch=sum(weight_total)/1000) %>% 
#   arrange(desc(year)) %>% 
#   collect(n=Inf)

mfdb_import_survey(mdb,
                   data_source = 'shrimp.commercial.landings',
                   data_in = landed_catch_shrimp %>% 
                     #select(-r) %>% 
                     mutate(vessel =ifelse(vessel=='-0',NA,vessel)) %>% 
                     ## this is a hotfix due to importing problem and the landings are ~100 kg from these vessels
                     filter(!(vessel %in% c('1694-0','5000-0','5059-0',
                                            '5688-0','5721-0','8076-0','8091-0','9083-0')),
                            weight_total >0,
                            !is.na(weight_total)) %>% 
                     collect(n=Inf) %>% 
                     as.data.frame())


