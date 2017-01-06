## devtools::install_github('mareframe/mfdb',ref='4.x')
library(mfdb)
library(geo)
library(tidyverse)
library(dplyrOracle)
library(mar)
library(purrr)
library(tidyr)
library(stringr)


## oracle connection from https://github.com/tomasgreif/dplyrOracle
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
stations <-
  lesa_stodvar(mar) %>% 
  filter(synaflokkur %in% c(1,2,8,10,12,30,34,35)) %>% 
  mutate(sampling_type = ifelse(synaflokkur %in% c(1,2,8),'SEA',
                                ifelse(synaflokkur %in% c(10,12),'CAP',
                                       ifelse(synaflokkur == 30,'IGFS',
                                              ifelse(synaflokkur==35,'AUT','SMN')))),
         man = ifelse(synaflokkur == 30, 3,
                      ifelse(synaflokkur == 35,10,man))) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>% 
  #mutate(smareitur = nvl(smareitur,10*reitur)) %>% 
  select(synis_id,ar,man,lat=kastad_n_breidd,lon=kastad_v_lengd,lat1=hift_n_breidd,lon1=hift_v_lengd,
         gear,sampling_type,depth=dypi_kastad,smareitur,vessel = skip) %>% 
  #mutate(lat = nvl(lat,sr2d(smareitur,1)*10000),
  #       lon = nvl(lon,sr2d(smareitur,0)*10000)) %>% 
  select(-smareitur) %>% 
  mutate(lat=nvl(lat,0),lon=nvl(lon,0)) %>% 
  mutate(areacell=d2sr(lat,lon),
         lon1 = nvl(lon1,lon),
         lat1 = nvl(lat1,lat)) %>% 
  filter(sampling_type=='IGFS') %>%
#  mutate(length = arcdist(lat,lon,lat1,lon1)) %>%  
  select(-c(lat1,lon1)) %>% 
  inner_join(tbl(mar,'reitmapping') %>% 
               select(areacell=GRIDCELL),
             by='areacell') %>% 
  rename(tow_id=synis_id,year = ar, month = man,latitude =lat,longitude = lon)

## Skoða seinna
if(FALSE){
## information on tows
  stations %>% 
    collect(n=Inf) %>% 
    as.data.frame() %>% 
    mfdb_import_tow_taxonomy(mdb,.)
  
  ## information on vessels
  tmp <- 
    mar:::ora_table(mar,'kvoti.skipasaga') %>% 
    left_join(mar:::ora_table(mar,'kvoti.skip_extra') %>% 
                select(skip_nr, power=orka_velar_1)) %>% 
    left_join(mar:::ora_table(mar,'kvoti.utg_fl') %>% 
                select(flokkur,vessel_type = heiti,enskt_heiti)) %>% 
    select(name=skip_nr,saga_nr,vessel_type,tonnage = brl,power,
           full_name = heiti,length=lengd,enskt_heiti) %>% 
    collect(n=Inf) %>% 
    as.data.frame() %>% 
    mfdb_import_vessel_taxonomy(mdb,.)
  
}

## length distributions
ldist <- 
  lesa_lengdir(mar) %>%
  inner_join(tbl(mar,'species_key')) %>% 
  skala_med_toldum() %>% 
  rename(tow_id=synis_id) %>%  
  right_join(stations) %>% 
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
                   ldist %>% select(-vessel))
rm(ldist)

## age -- length data
aldist <-
  lesa_kvarnir(mar) %>% 
  rename(tow_id=synis_id) %>% 
  right_join(stations) %>% 
  inner_join(tbl(mar,'species_key')) %>%
  mutate(lengd = ifelse(is.na(lengd), 0, lengd),
         count = 1,
         kyn = ifelse(kyn == 2,'F',ifelse(kyn ==1,'M',NA)),
         kynthroski = ifelse(kynthroski > 1,2,ifelse(kynthroski == 1,1,NA)))%>%
  select(tow_id, latitude,longitude, year,month, areacell, gear,
         sampling_type,count,species,
         age=aldur,sex=kyn,maturity_stage = kynthroski,
         length = lengd, no = nr, weight = oslaegt,
         gutted = slaegt, liver = lifur, gonad = kynfaeri) %>% 
  collect(n=Inf) %>% 
  as.data.frame()


mfdb_import_survey(mdb,
                   data_source = 'iceland-aldist',
                   aldist)
rm(aldist)
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


landed_catch <- 
  mar:::lods_oslaegt(mar) %>%
  filter(ar > 1993) %>% 
  select(veidarfaeri, skip_nr,  fteg,
         ar, man, hofn, magn_oslaegt, veidisvaedi, l_dags) %>% 
  dplyr::union_all(tbl(mar,'landed_catch_pre94') %>% 
                     mutate(l_dags = to_date(concat(ar,man),'yyyymm'))) %>% 
  left_join(tbl_mar(mar,'kvoti.skipasaga'), by='skip_nr') %>% 
  mutate(ur_gildi = nvl(ur_gildi,to_date('01.01.2099','dd.mm.yyyy')),
         i_gildi =  nvl(i_gildi,to_date('01.01.1970','dd.mm.yyyy'))) %>% 
  filter(l_dags < ur_gildi, l_dags > i_gildi) %>% 
  #select(skip_nr,hofn,l_dags,saga_nr,gerd,fteg,kfteg,veidisvaedi,stada,veidarfaeri,magn_oslaegt, i_gildi,flokkur,ar,man) %>% 
  filter(veidisvaedi == 'I',nvl(flokkur,0) != -4) %>% 
  left_join( tbl(mar,'gear_mapping'),by='veidarfaeri') %>%
  inner_join( tbl(mar,'species_key'),by=c('fteg'='tegund')) %>%
  left_join(tbl(mar,'port2sr'),by='hofn') %>%
  mutate(sampling_type='LND',
         gear = ifelse(is.na(gear),'LLN',gear)) %>% 
  select(count=magn_oslaegt,sampling_type,areacell, species,year=ar,month=man,
         gear) 


landed_catch %>% 
  filter(species == 'LIN') %>% 
  group_by(year) %>% 
  #summarise(catch=sum(ifelse(year>1995,count,count/0.8))/1000) %>% 
  summarise(catch=sum(count)/1000) %>% 
  arrange(desc(year)) %>% collect(n=Inf) -> tmp
  View()




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


landingsByYear %>% filter(species == 'ling') %>% 
  rename(year=Year) %>% 
  left_join(tmp) %>% 
  filter(year %in% 1994:1995) %>% 
  select(year,Iceland,catch) %>% 
  mutate(r=catch/Iceland)
  ggplot(aes(year,catch/Iceland)) + geom_line() 


landingsByMonth <-
  landingsByYear %>%
  filter(species!='capelin') %>% 
  left_join(spitToDST2) %>%
  inner_join(tbl(mar,'species_key') %>% collect(), by = c('shortname'='species')) %>% 
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



mfdb_import_survey(mdb,
                   data_source = 'foreign.landings',
                   landingsByMonth)

oldLandingsByMonth <-
  landingsByYear %>%
  left_join(spitToDST2) %>%
  inner_join(tbl(mar,'species_key') %>% collect(), by = c('shortname'='species')) %>%
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
