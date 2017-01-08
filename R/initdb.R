## devtools::install_github('mareframe/mfdb',ref='4.x')
library(mfdb)
library(geo)
library(tidyverse)
library(mar)
library(purrr)
library(tidyr)
library(stringr)


## oracle connection from https://github.com/fishvice/dplyrOracle
mar <- dplyrOracle::src_oracle("mar")

## Create connection to MFDB database, as the Icelandic case study
mdb <- mfdb('Iceland')#,db_params = list(host='hafgeimur.hafro.is'))

## Import area definitions
reitmapping <- read.table(
  system.file("demo-data", "reitmapping.tsv", package="mfdb"),
  header=TRUE,
  as.is=TRUE) %>% 
  tbl_df() %>% 
  mutate(id = 1:n(),
         lat = geo::sr2d(GRIDCELL)$lat,
         lon = geo::sr2d(GRIDCELL)$lon) %>% 
  by_row(safely(function(x) geo::srA(x$GRIDCELL),otherwise=NA)) %>% 
  unnest(size=.out %>% map('result')) %>% 
  select(-.out) %>% 
  na.omit()
db_drop_table(mar$con,'reitmapping')
copy_to(mar,reitmapping,'reitmapping',overwrite=TRUE)

mfdb_import_area(mdb, 
                 reitmapping %>% 
                   select(id,
                          name=GRIDCELL,
                          size)) 

mfdb_import_division(mdb,
                     plyr::dlply(reitmapping,
                                 'SUBDIVISION',
                                 function(x) x$GRIDCELL))

mfdb_import_temperature(mdb, 
                        expand.grid(year=1960:2016,
                                    month=1:12,
                                    areacell = reitmapping$GRIDCELL,
                                    temperature = 3))

## BEGIN hack
## species mapping
data.frame(tegund =  c(1:11, 19, 21, 22,
                       23, 25, 27, 30, 31, 48 ),
           species = c('COD','HAD','POK','WHG','RED','LIN','BLI','USK',
                       'CAA','RNG','REB','GSS','HAL','GLH',
                       'PLE','WIT','DAB','HER','CAP','LUM')) %>% 
  copy_to(mar,.,'species_key')

## gear mapping
ice.gear <-
  read.table('inst/veidarf.txt',header=TRUE)

mapping <- read.table(header=TRUE,
                      file='inst/mapping.txt') 

mapping <-
  mutate(merge(mapping, gear, by.x='gear',
               by.y = 'id'),
         gear=NULL,
         description=NULL)
names(mapping)[2] <- 'gear'
mapping$gear <- as.character(mapping$gear)
db_drop_table(mar$con,'gear_mapping')
copy_to(dest = mar, 
        df = mapping,
        name = 'gear_mapping')

## Set-up sampling types

mfdb_import_sampling_type(mdb, data.frame(
  id = 1:16,
  name = c('SEA', 'IGFS','AUT','SMN','LND','LOG','INS','ACU','FLND','OLND','CAA',
           'CAP','GRE','FAER','RED','RAC'),
  description = c('Sea sampling', 'Icelandic ground fish survey',
                  'Icelandic autumn survey','Icelandic gillnet survey',
                  'Landings','Logbooks','Icelandic nephrop survey',
                  'Acoustic capelin survey','Foreign vessel landings','Old landings (pre 1981)',
                  'Old catch at age','Capelin data','Eastern Greenland autumn survey',
                  'Faeroese summer survey','Redfish survey','Redfish accoustic survey')))

## stations table

## Import length distribution from commercial catches
## 1 inspectors, 2 hafro, 8 on-board discard
vessel_map <- 
  lesa_stodvar(mar) %>% 
  select(synis_id,dags,skip) %>% 
  left_join(tbl_mar(mar,'kvoti.skipasaga') %>% 
              select(skip=skip_nr,saga_nr,i_gildi,ur_gildi)) %>%
  filter((dags > i_gildi & dags <= ur_gildi) | is.na(skip) | is.na(i_gildi)) %>% 
  select(synis_id,skip,saga_nr)


stations <-
  lesa_stodvar(mar) %>% 
  left_join(vessel_map) %>% 
  mutate(saga_nr = nvl(saga_nr,0)) %>% 
  filter(synaflokkur %in% c(1,2,8,10,12,30,34,35)) %>% 
  mutate(sampling_type = ifelse(synaflokkur %in% c(1,2,8),'SEA',
                                ifelse(synaflokkur %in% c(10,12),'CAP',
                                       ifelse(synaflokkur == 30,'IGFS',
                                              ifelse(synaflokkur==35,'AUT','SMN')))),
         man = ifelse(synaflokkur == 30, 3,
                      ifelse(synaflokkur == 35,10,man)),
         institute = 'MRI',
         vessel = concat(concat(skip,'-'),saga_nr)) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>% 
  select(synis_id,ar,man,lat=kastad_n_breidd,lon=kastad_v_lengd,lat1=hift_n_breidd,lon1=hift_v_lengd,
         gear,sampling_type,depth=dypi_kastad,vessel) %>% 
  mutate(lat=nvl(lat,0),lon=nvl(lon,0)) %>% 
  mutate(areacell=d2sr(lat,lon),
         lon1 = nvl(lon1,lon),
         lat1 = nvl(lat1,lat)) %>% 
  filter(sampling_type=='IGFS') %>%
  mutate(length = arcdist(lat,lon,lat1,lon1)) %>%
#  mutate(length = 4) %>% 
  select(-c(lat1,lon1)) %>% 
  inner_join(tbl(mar,'reitmapping') %>% 
               select(areacell=GRIDCELL),
             by='areacell') %>% 
  rename(tow=synis_id,year = ar, month = man,latitude =lat,longitude = lon)


## information on tows
  stations %>% 
    rename(name = tow) %>% 
    collect(n=Inf) %>% 
    as.data.frame() %>% 
    mfdb_import_tow_taxonomy(mdb,.)
  
  ## information on vessels
  
vessel_type_new <- 
  read_csv('inst/vessel_type.csv') %>% 
  rename(name=vessel_type) %>% 
  mutate(id=1:n())
  
mfdb:::mfdb_import_taxonomy(mdb,'vessel_type',vessel_type_new)  

  tmp <- 
    tbl_mar(mar,'kvoti.skipasaga') %>% 
    left_join(tbl_mar(mar,'kvoti.skip_extra') %>% 
                select(skip_nr, power=orka_velar_1)) %>% 
    left_join(tbl_mar(mar,'kvoti.utg_fl') %>% 
                mutate(vessel_type = decode(flokkur,-6,'GOV',-4,'FGN',
                                            -3,'NON',
                                            0,'NON',
                                            1,'NON',
                                            3,'RSH',
                                            11,'FRZ',
                                            99,'JIG',
                                            98,'JIG',
                                            -8,'DIV',
                                            100,'JIG',
                                            101,'JIG',
                                            6,'JIG',
                                            'COM')) %>% 
                select(flokkur,vessel_type)) %>% 
    mutate(name = concat(concat(skip_nr,'-'),saga_nr)) %>% 
    select(name,vessel_type,tonnage = brl,power,
           full_name = heiti,length=lengd) %>% 
    collect(n=Inf) %>% 
    as.data.frame() %>% 
    mfdb_import_vessel_taxonomy(mdb,.)
  
  
  
mfdb_import_vessel_taxonomy(mdb,data.frame(name='-0',length=NA,tonnage=NA,power=NA,full_name = 'Unknown vessel'))
  
  
## length distributions
ldist <- 
  lesa_lengdir(mar) %>%
  inner_join(tbl(mar,'species_key')) %>% 
  skala_med_toldum() %>% 
  rename(tow=synis_id) %>%  
  right_join(stations %>% 
               select(-length)) %>% 
  mutate(lengd = ifelse(is.na(lengd), 0, lengd),
         fjoldi = ifelse(is.na(fjoldi), 0, fjoldi),
         r = ifelse(is.na(r), 1 , r),
         kyn = ifelse(kyn == 2,'F',ifelse(kyn ==1,'M','')),
         kynthroski = ifelse(kynthroski > 1,2,ifelse(kynthroski == 1,1,NA)),
         age = 0)%>%
  select(-c(fjoldi,r,tegund)) %>% 
  rename(sex=kyn,maturity_stage = kynthroski,
         length = lengd) %>% 
  collect(n=Inf) %>% 
  as.data.frame()


mfdb_import_survey(mdb,
                   data_source = 'iceland-ldist',
                   ldist)

## age -- length data
aldist <-
  lesa_kvarnir(mar) %>% 
  rename(tow=synis_id) %>% 
  right_join(stations %>% 
               select(-length)) %>%
  inner_join(tbl(mar,'species_key')) %>%
  mutate(lengd = ifelse(is.na(lengd), 0, lengd),
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
                   data_source = 'iceland-aldist',
                   aldist)

## landings 
port2division <- function(hofn){
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
  data.frame(hofn=hofn,division=hafnir.numer) 
}

gridcell.mapping <-
  plyr::ddply(reitmapping,~DIVISION,function(x) head(x,1)) %>% 
  rename(areacell=GRIDCELL,division=DIVISION,subdivision=SUBDIVISION)
db_drop_table(mar$con,'port2sr')
port2division(0:999) %>% 
  left_join(gridcell.mapping) %>% 
  copy_to(mar,.,'port2sr')    

## landing pre 1994 from fiskifélagið

db_drop_table(mar$con,'landed_catch_pre94') 
bind_rows(
  list.files('/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = '^[0-9]+',full.names = TRUE) %>% 
    map(~read.table(.,skip=2,stringsAsFactors = FALSE,sep='\t')) %>% 
    bind_rows() %>% 
    rename_(.dots=stats::setNames(colnames(.),c('vf',	'skip',	'teg',	'ar',	'man',	'hofn',	'magn'))) %>% 
    mutate(magn=as.numeric(magn)),
  list.files('/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = 'ready',full.names = TRUE) %>% 
    map(~read.table(.,skip=2,stringsAsFactors = FALSE,sep='\t')) %>% 
    bind_rows() %>% 
    rename_(.dots=stats::setNames(colnames(.),c(	'ar','hofn',	'man',	'vf',	'teg', 'magn'))),
  list.files('/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = 'afli.[0-9]+$',full.names = TRUE) %>% 
    map(~read.table(.,skip=2,stringsAsFactors = FALSE,sep=';')) %>% 
    bind_rows()%>% 
    rename_(.dots=stats::setNames(colnames(.),c(	'ar','hofn',	'man',	'vf',	'teg', 'magn')))) %>%
  filter(!(ar==1991&is.na(skip))) %>% 
  mutate(veidisvaedi='I') %>% 
  rename(veidarfaeri=vf,skip_nr=skip,magn_oslaegt=magn,fteg=teg) %>% 
  copy_to(mar,.,'landed_catch_pre94')



## catches 
landings_map <- 
  mar:::lods_oslaegt(mar) %>%
  left_join(tbl_mar(mar,'kvoti.skipasaga'), by='skip_nr') %>% 
  filter(l_dags < ur_gildi, l_dags > i_gildi) %>% 
  select(skip_nr,saga_nr,komunr,hofn) %>% 
  distinct()

landed_catch <- 
  mar:::lods_oslaegt(mar) %>%   
  left_join(landings_map) %>% 
  filter(ar > 1993) %>% 
  select(veidarfaeri, skip_nr, fteg,
         ar, man, hofn, magn_oslaegt, 
         veidisvaedi, l_dags, saga_nr) %>% 
  dplyr::union_all(tbl(mar,'landed_catch_pre94') %>% 
                     mutate(l_dags = to_date(concat(ar,man),'yyyymm'),
                            saga_nr = 0)) %>%
  left_join(tbl_mar(mar,'kvoti.skipasaga'), by=c('skip_nr','saga_nr')) %>% 
  mutate(vessel = concat(concat(nvl(skip_nr,''),'-'),nvl(saga_nr,0)),
         flokkur = nvl(flokkur,0)) %>% 
  #select(skip_nr,hofn,l_dags,saga_nr,gerd,fteg,kfteg,veidisvaedi,stada,veidarfaeri,magn_oslaegt, i_gildi,flokkur,ar,man) %>% 
  filter(veidisvaedi == 'I',flokkur != -4) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>%
  inner_join(tbl(mar,'species_key'),by=c('fteg'='tegund')) %>%
  left_join(tbl(mar,'port2sr'),by='hofn') %>%
  mutate(sampling_type='LND',
         gear = ifelse(is.na(gear),'LLN',gear)) %>% 
  select(count=magn_oslaegt,sampling_type,areacell, vessel,species,year=ar,month=man,
         gear)



foreign_landed_catch <- 
  mar:::lods_oslaegt(mar) %>%   
  left_join(landings_map) %>% 
  filter(ar > 2013) %>% 
  select(veidarfaeri, skip_nr, fteg,
         ar, man, hofn, magn_oslaegt, 
         veidisvaedi, l_dags, saga_nr) %>% 
  left_join(tbl_mar(mar,'kvoti.skipasaga'), by=c('skip_nr','saga_nr')) %>% 
  mutate(vessel = concat(concat(nvl(skip_nr,''),'-'),nvl(saga_nr,0)),
         flokkur = nvl(flokkur,0)) %>% 
  #select(skip_nr,hofn,l_dags,saga_nr,gerd,fteg,kfteg,veidisvaedi,stada,veidarfaeri,magn_oslaegt, i_gildi,flokkur,ar,man) %>% 
  filter(veidisvaedi == 'I',flokkur == -4) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>%
  inner_join(tbl(mar,'species_key'),by=c('fteg'='tegund')) %>%
  left_join(tbl(mar,'port2sr'),by='hofn') %>%
  mutate(sampling_type='FLND',
         gear = ifelse(is.na(gear),'LLN',gear)) %>% 
  select(count=magn_oslaegt,sampling_type,areacell, vessel,species,year=ar,month=man,
         gear)

mfdb_import_survey(mdb,data_source = 'lods.foreign.landings',
                   foreign_landed_catch %>% collect(n=Inf) %>% as.data.frame())

url <- 'http://data.hafro.is/assmt/2016/'

x1 <- gsub("<([[:alpha:]][[:alnum:]]*)(.[^>]*)>([.^<]*)", "\\3",
           readLines(url))
x2<-gsub("</a>", "", x1)
sp.it <- sapply(strsplit(x2[grepl('/ ',x2)],'/'),function(x) str_trim(x[1]))[-1]
spitToDST2 <- 
  read.table(text = 
               paste('species shortname',
                     'anglerfish MON',
                     'b_ling BLI',
                     'b_whiting WHB',
                     'capelin CAP',
                     'cod COD',
                     'cucumber HTZ',
                     'dab DAB',
                     'g_halibut GLH',
                     'haddock HAD',
                     'halibut HAL',
                     'herring HER',
                     'l_sole LEM',
                     'ling LIN',
                     'lumpfish LUM',
                     'mackerel MAC',
                     'megrim MEG',
                     'nephrops NEP',
                     'plaice PLE',
                     'quahog QUA',
                     'r_dab DAB',
                     's_mentella REB',
                     's_norvegicus RED',
                     's_smelt GSS',
                     's_viviparus REV',
                     's_wolfish CAA',
                     'saithe POK',
                     'scallop CYS',
                     'tusk USK',
                     'urchin EEZ',
                     'whelk CSI',
                     'whiting WHG',
                     'witch WIT',
                     'wolffish CAA',
                     sep = '\n'),
             header = TRUE)



landingsByYear <-
  plyr::ldply(sp.it, function(x){
    #print(x)
    tmp <- tryCatch(read.csv(sprintf('%s%s/landings.csv',url,x)),
                    error=function(x) data.frame(Total=0))
    tmp$species <- x
    
    #print(names(tmp))
    return(tmp)
  })


landed_catch %>% 
  group_by(species,year) %>% 
  #summarise(catch=sum(ifelse(year>1995,count,count/0.8))/1000) %>% 
  summarise(catch=sum(count)/1000) %>% 
  arrange(desc(year)) %>% collect(n=Inf) -> tmp



ling_tusk_scalar <- 
  landingsByYear %>%
  filter(species %in% c('tusk','ling')) %>% 
  left_join(spitToDST2) %>% 
  select(Year:Total,species=shortname)%>% 
  rename(year=Year) %>% inner_join(tmp) %>%
  mutate(r=Iceland/catch) %>% 
  filter(year %in% 1993:1996) %>% 
  select(year,species,r)

landings <- 
  landed_catch %>% 
  collect(n=Inf) %>% 
  left_join(ling_tusk_scalar) %>% 
  mutate(count = ifelse(is.na(r),count,r*count)) 

mfdb_import_survey(mdb,
                   data_source = 'commercial.landings',
                   data_in = landings %>% 
                     select(-r) %>% 
                     filter(!(vessel %in% c('1694-0','5000-0','5059-0',
                                            '5688-0','5721-0','8076-0','8091-0','9083-0')),
                            count >0,
                            !is.na(count)) %>% 
                     as.data.frame())



landingsByMonth <-
  landingsByYear %>%
  filter(species!='capelin') %>% 
  left_join(spitToDST2) %>%
  #inner_join(tbl(mar,'species_key') %>% collect(), by = c('shortname'='species')) %>% 
  filter(!is.na(shortname) & !is.na(Year)) %>%
  select(shortname,Year,Iceland,Total) %>%
  left_join((expand.grid(Year=1905:2015,month=1:12))) %>%
  mutate(year = Year,
         sampling_type = 'FLND', 
         species = shortname,
         Others = Total - Iceland,
         count = 1000*Others/12,
         gear = 'LLN', ## this needs serious consideration
         areacell = 2741) %>% ## just to have something
  mutate(shortname = NULL,
         Others = NULL,
         Total = NULL) %>% 
  as.data.frame()


landingsByYear %>%
  filter(species!='capelin') %>% 
  left_join(spitToDST2) %>% 
  select(Year,Iceland) %>% 


mfdb_import_survey(mdb,
                   data_source = 'foreign.landings',
                   landingsByMonth %>% filter(species != 'QUA',year< 2014))

oldLandingsByMonth <-
  landingsByYear %>%
  left_join(spitToDST2) %>%
#  inner_join(tbl(mar,'species_key') %>% collect(), by = c('shortname'='species')) %>%
  filter(!is.na(shortname) & !is.na(Year) &  Year < 1982) %>%
  select(shortname,Year,Others,Total) %>%
  left_join(expand.grid(Year=1905:1981,month=1:12)) %>%
  mutate(year = Year,
         sampling_type = 'OLND', 
         species = shortname,
         count = 1000*(Total - Others)/12,
         gear = 'BMT', ## this needs serious consideration
         areacell = 2741) %>% ## just to have something
  mutate(shortname = NULL,
         Others = NULL,
         Total = NULL) %>% 
  as.data.frame()

mfdb_import_survey(mdb,
                   data_source = 'old.landings',
                   oldLandingsByMonth)
