#devtools::install_github('mareframe/mfdb',ref='6.x')
#devtools::install_github('fishvice/mar', force = TRUE)
#devtools::install_github('Hafro/rgadget')
#install.packages("tidyverse")
library(mfdb)
library(tidyverse)
library(mar)
library(purrr)
library(purrrlyr)
library(tidyr)
library(stringr)
library(ROracle)
library(dbplyr)


## oracle connection 
mar <- dbConnect(dbDriver('Oracle'))

## Create connection to MFDB database, as the Icelandic case study
mdb <- mfdb('Iceland')#, destroy_schema = TRUE)#,db_params = list(host='hafgeimur.hafro.is'))
#after destroying the schema, the mfdb package needs to be reloaded 

source("shrimp_support_tables.R")


#DOUBLE CHECK WITH INGA THAT EXTRA ISAFJORDURDJUP TOWS STILL INCLUDED -
# WILL NEED TO REDO THE #TOWS TO FORM BIOMASS INDEX IF NOT, OR # MAY CHANGE A FEW YEARS AGO
# ALSO - WHY ARE THERE SO MANY MONTHS? TRANSLATED OK INTO SEASONS?


## Import area definitions
reitmapping_original <- read.table(
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
  mutate(id = max(reitmapping_original$id) + 1:n())

reitmapping <-
  reitmapping_original %>%
  transmute(GRIDCELL = as.character(GRIDCELL), DIVISION, SUBDIVISION = as.character(SUBDIVISION), id, lat, lon, size) %>% 
  full_join(transmute(reitmapping_fjords, GRIDCELL, DIVISION, SUBDIVISION, id, size)) 

dbWriteTable(mar,'reitmapping',as.data.frame(reitmapping),overwrite=TRUE)

## import the area definitions into mfdb
mfdb_import_area(mdb, 
                 reitmapping %>% 
                   select(id,
                          name=GRIDCELL,
                          size) %>% 
                   as.data.frame()) 

## create area division (used for e.g. bootstrapping)
mfdb_import_division(mdb,
                     plyr::dlply(reitmapping,
                                 'SUBDIVISION',
                                 function(x) x$GRIDCELL))

## here some work on temperature would be nice to have instead of fixing the temperature
## should probably include depth
mfdb_import_temperature(mdb, 
                        expand.grid(year=1960:2020,
                                    month=1:12,
                                    areacell = reitmapping$GRIDCELL,
                                    temperature = 3,
                                    depth = 0))


## BEGIN hack
## species mapping
data.frame(tegund =  c(1:11, 19, 21, 22,
                       23, 25, 27, 30, 31, 48, 14, 24, 41),
           species = c('COD','HAD','POK','WHG','RED','LIN','BLI','USK',
                       'CAA','RNG','REB','GSS','HAL','GLH',
                       'PLE','WIT','DAB','HER','CAP','LUM','MON','LEM','SHR')) %>% 
  dbWriteTable(mar,'species_key',.,overwrite = TRUE)

## gear mapping
ice.gear <-
  read.table('https://raw.githubusercontent.com/fishvice/mfdb-hafro-etl/master/inst/veidarf.txt',header=TRUE)

mapping <- read.table(header=TRUE,
                      file='https://raw.githubusercontent.com/fishvice/mfdb-hafro-etl/master/inst/mapping.txt') 


mapping <-
  mutate(merge(mapping, gear, by.x='gear',
               by.y = 'id'),
         gear=NULL,
         description=NULL)
names(mapping)[2] <- 'gear'
mapping$gear <- as.character(mapping$gear)

#distinguish gears for shrimp so that data can be subsetted later to distinguish selectivity
mapping <-
  mapping %>% 
  mutate(gear = ifelse(veidarfaeri==30, 'TMS', gear))

dbWriteTable(conn = mar, 
             value = mapping,
             name = 'gear_mapping',
             overwrite = TRUE)

#mfdb:::mfdb_import_taxonomy(mdb, table_name = "gear", data_in = data.frame(id = 1141, name = 'SHT.B', description = 'shrimp trawl with end bag'))
#try to empty and rebuild it
#or try to find it within githunb

#mfdb:::mfdb_import_cs_taxonomy(mdb, 'gear', data.frame(id = 1141, name = 'SHT.B', description = 'shrimp trawl with end bag', t_group = NA))
#mfdb:::mfdb_import_gear_taxonomy(mdb, data.frame(id = 1141, name = 'SHT.B', description = 'shrimp trawl with end bag', t_group = NA))
#in mfdb/demo/example-minouw-size_fr
#gear %>% View()

## Set-up sampling types

mfdb_import_sampling_type(mdb, data.frame(
  id = 1:21,
  name = c('SEA', 'IGFS','AUT','SMN','LND','LOG','INS','OFS','XINS','XS','ACU','FLND','OLND','CAA',
           'CAP','GRE','FAER','RED','RAC','LOBS','HYD'),
  description = c('Sea sampling', 'Icelandic groundfish survey',
                  'Icelandic autumn survey','Icelandic gillnet survey',
                  'Landings','Logbooks','Inshore shrimp survey', 'Offshore shrimp survey',
                  'Extra-survey inshore shrimp samples', 'Extra samples',
                  'Acoustic capelin survey','Foreign vessel landings','Old landings (pre 1981)',
                  'Old catch at age','Capelin data','Eastern Greenland autumn survey',
                  'Faeroese summer survey','Redfish survey','Redfish accoustic survey',
                  'Icelandic nephrops survey','Ocean or hydrographic data')))

## stations table

## Import length distribution from commercial catches
## 1 inspectors, 2 hafro, 8 on-board discard
dbRemoveTable(mar,'vessel_map')
vessel_map <- 
  lesa_stodvar(mar) %>% 
  select(synis_id,dags,skip) %>% 
  left_join(tbl_mar(mar,'kvoti.skipasaga') %>% 
              select(skip=skip_nr,saga_nr,i_gildi,ur_gildi)) %>%
  filter((dags > i_gildi & dags <= ur_gildi) | nvl(skip,-999)==-999 | nvl(i_gildi,to_date('01.01.2100','dd.mm.yyyy'))==to_date('01.01.2100','dd.mm.yyyy')) %>% 
  select(synis_id,skip,saga_nr) %>% 
  compute(name='vessel_map',temporary=FALSE)

stations <-
  lesa_stodvar(mar) %>% 
  left_join(tbl(mar,'vessel_map')) %>% 
  #left_join(tbl(mar,'catch_map')) %>% 
  mutate(saga_nr = nvl(saga_nr,0),
         ar = to_number(to_char(dags,'yyyy')),
         man = to_number(to_char(dags,'mm'))) %>% 
  shrimp_station_fixes() %>%
  filter(synaflokkur %in% c(1,2,8,10,12,14,20,30,31,34,35,37,38)) %>% 
  mutate(sampling_type = ifelse(synaflokkur %in% c(1,2,8),'SEA',
                                ifelse(synaflokkur %in% c(10,12),'CAP',
                                       ifelse(synaflokkur == 30,'IGFS',
                                              ifelse(synaflokkur==35,'AUT',
                                                     #INS is Stofnmæling innfjarðarrækju, XINS are 37s that are extra surveys.
                                                     ifelse(synaflokkur==37,'INS',
                                                            #14 renamed from 37, tows not part of survey design
                                                            ifelse(synaflokkur==14,'XINS',
                                                                   #Nytjastofnarannsóknir - leiguskip is 20
                                                                   ifelse(synaflokkur==20,'XS',
                                                                    #OFS is Stofnmæling úthafsrækju
                                                                    ifelse(synaflokkur==31,'OFS',
                                                                          ifelse(synaflokkur==38,'LOBS','SMN'))))))))),
         ## This is a March survey but Gadget stocks recruit after being predated, -> bump the timing to next quarter
         man = ifelse(synaflokkur == 30, 4,   
                      ifelse(synaflokkur == 35,10,man)),
         institute = 'MRI',
         vessel = concat(concat(skip,'-'),saga_nr)) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>% 
  select(synis_id,ar,man,lat=kastad_n_breidd,lon=kastad_v_lengd,lat1=hift_n_breidd,lon1=hift_v_lengd,
         gear,sampling_type,depth=dypi_kastad,vessel,reitur,smareitur,skiki,fjardarreitur,
         leidangur,toglengd,tognumer) %>% 
  mutate(areacell=ifelse(sampling_type %in% c('INS', 'XINS', 'XS'), concat(concat(skiki,'_'),fjardarreitur), as.character(10*reitur+nvl(smareitur,1)))
         ) %>%  #not sure that XS sampling type should use fjardarreitur for area size definition
  mutate(towlength = arcdist(lat,lon,lat1,lon1), season = ifelse(man %in% 1:2,1,ifelse(man %in% 3:5, 2, ifelse(man %in% 9:10, 3, 4)))) %>%
  ##why is length taken from lat / long rather than toglengd? Former method causes 26 rows of INS data to have length = 0
  distinct() %>% 
  group_by(ar, fjardarreitur, skiki, season) %>%
  mutate(towcount = ifelse(sampling_type=='INS' & is.na(fjardarreitur)==0 & is.na(tognumer)==0, n(), NA)) %>% 
  ungroup() %>% 
  select(-c(lat1,lon1,reitur,smareitur,fjardarreitur,skiki,leidangur, season)) %>% 
  inner_join(tbl(mar,'reitmapping') %>% 
               select(areacell=GRIDCELL, size),
             by='areacell') %>% 
  rename(tow=synis_id) %>% 
  rename(towlength_original = toglengd) %>% 
  rename(month = man) %>% 
  rename(year = ar) %>% 
  rename(latitude =lat) %>% 
  rename(longitude = lon)

dbRemoveTable(mar,'stations')
stations %>% 
  compute(name='stations',temporary=FALSE,indexes = list('tow'))

#IF AN ERROR IS GENERATED HERE, THEN THE SCHEMA NEEDS TO BE DESTROYED WHEN MDB DEFINED
## information on tows
tbl(mar,'stations') %>% 
  rename(name = tow) %>% 
  collect(n=Inf) %>% 
  as.data.frame() %>%
  mutate(name = ifelse(name == 1e5, '100000',ifelse(name == 4e5,'400000',as.character(name)))) %>%
  mfdb_import_tow_taxonomy(mdb,.)

## information on vessels
vessel_type_new <- 
  read_csv('https://raw.githubusercontent.com/fishvice/mfdb-hafro-etl/master/inst/vessel_type.csv') %>% 
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
dbRemoveTable(mar,'ldist')
lesa_lengdir(mar) %>% 
  inner_join(tbl(mar,'species_key')) %>%  #as.tibble() %>% View()
  skala_med_toldum2() %>% glimpse
  rename(tow=synis_id) %>% 
  compute(name='ldist',temporary=FALSE)
  
#could be that SEA samples have weight not correct here
ldist <- 
  tbl(mar,'ldist') %>% 
  right_join(tbl(mar,'stations'), #%>% 
               #select(-towlength), #no longer removed - retained for biomass index
             by = 'tow') %>% 
  mutate(lengd = nvl(lengd,0),
         lengd = ifelse(lengd > 4 & tegund == 41, lengd/10, lengd), #fixes 2017 shrimp entered in mm
         fjoldi = nvl(fjoldi,0), 
         kyn = ifelse(kyn == 2,'F',ifelse(kyn ==1,'M','')),
         kynthroski = ifelse(kynthroski > 1,2,ifelse(kynthroski == 1,1,NA)),
         age = 0,
         weight = ifelse(is.na(mean_wt)==1 | is.na(towlength_original)==1 | is.na(towcount)==1 | is.na(size)==1, 
                           NA, (mean_wt/towlength_original)/towcount*size)) %>% 
  select(-c(r,biom.r,tegund, mean_wt, towcount, size, towlength, towlength_original)) %>% 
  rename(sex=kyn) %>% 
  rename(maturity_stage = kynthroski) %>% 
  rename(length = lengd) %>% 
  rename(count=fjoldi) %>% 
  collect(n=Inf) %>% 
  as.data.frame()


## this is just a fix for vessels not in skipasaga
mfdb_import_vessel_taxonomy(mdb,
                            data.frame(name=c('1010-0','1015-0','1018-0','1026-0','1027-0','103-0','1034-0','1038-0','104-0','1041-0',
                                              '105-0','1050-0','1058-0','1059-0','1065-0','1069-0','1085-0','1086-0','109-0','1096-0',
                                              '1098-0','1099-0','110-0','1101-0','1108-0','111-0','1110-0','1116-0','1119-0','1122-0',
                                              '1123-0','1137-0','1138-0','1145-0','115-0','1158-0','1160-0','1161-0','1172-0','1176-0',
                                              '1191-0','1194-0','1203-0','1208-0','121-0','1212-0','1215-0','1216-0','1217-0','122-0',
                                              '1223-0','1229-0','1247-0','1251-0','1253-0','1256-0','1259-0','126-0','128-0','1283-0',
                                              '1289-0','129-0','1301-0',
                                              '1309-0','131-0','1310-0','1313-0','1332-0','134-0','135-0','1355-0','136-0','1362-0',
                                              '1364-0','1384-0','14-0','1425-0','1442-0','1450-0','1455-0','1467-0','147-0','1488-0',
                                              '150-0','1503-0','152-0','153-0','1548-0','1550-0',
                                              '1557-0','1566-0','157-0','1592-0','16-0','160-0','1662-0','167-0','1673-0','171-0','1720-0',
                                              '179-0','18-0','181-0','186-0','1863-0','189-0','19-0','190-0','191-0','1934-0','194-0','197-0','198-0','201-0',
                                              '202-0','203-0','205-0','21-0','215-0','2198-0','22-0','223-0','227-0','228-0','234-0','235-0','240-0','247-0',
                                              '248-0','25-0','252-0','258-0','261-0','265-0','266-0','274-0','282-0','283-0','290-0','292-0','293-0','298-0',
                                              '30-0','315-0','32-0','321-0','322-0','326-0','327-0','33-0',
                                              '333-0','338-0','346-0','348-0','351-0','376-0','378-0','38-0','382-0','383-0','387-0','39-0','391-0','395-0',
                                              '398-0','40-0','405-0','41-0','413-0','416-0','419-0','421-0','422-0','424-0','426-0',
                                              '427-0','428-0','429-0','431-0','432-0','435-0','440-0','445-0','451-0','452-0','456-0',
                                              '457-0','458-0','459-0','469-0','474-0','475-0','478-0','480-0','481-0','484-0','486-0',
                                              '488-0','493-0','495-0',
                                              '497-0','50-0','503-0','504-0','509-0','519-0','52-0','520-0','521-0','526-0','53-0',
                                              '535-0','538-0','540-0','541-0','545-0','546-0','548-0','550-0','551-0','552-0',
                                              '556-0','558-0','561-0','562-0','569-0','57-0','570-0','571-0','574-0','58-0','580-0',
                                              '584-0','585-0','587-0','59-0','591-0','593-0','594-0','597-0','598-0','599-0','600-0',
                                              '607-0','608-0','609-0','611-0','613-0','615-0','627-0',
                                              '632-0','633-0','637-0','638-0','64-0','640-0','641-0','642-0','643-0','649-0',
                                              '65-0','650-0','651-0','6520-0','658-0','661-0','666-0','668-0','673-0','68-0',
                                              '680-0','682-0','688-0','69-0','700-0','702-0','704-0','706-0','707-0','7090-0',
                                              '710-0','712-0','715-0','719-0','720-0','721-0','722-0','7239-0','724-0','725-0',
                                              '735-0','736-0','740-0','7404-0','742-0','743-0','744-0','7493-0','75-0','757-0',
                                              '7572-0','760-0','763-0','7644-0','766-0','767-0','77-0','771-0','772-0','773-0',
                                              '7779-0','783-0','7833-0','785-0','786-0','788-0','79-0','790-0','791-0','797-0',
                                              '7982-0','8-0','80-0','800-0','8000-0','8001-0','8002-0','8003-0','8008-0','8009-0',
                                              '801-0','8010-0','8012-0','804-0','807-0','809-0','8101-0','811-0','812-0','815-0',
                                              '8180-0','8195-0','82-0','821-0','8213-0','822-0','827-0','8288-0','8368-0','839-0',
                                              '843-0','8456-0','846-0','8474-0','848-0','85-0','8534-0','8592-0','8610-0','862-0',
                                              '864-0','8651-0','867-0','868-0','873-0','874-0','876-0','877-0','878-0','879-0',
                                              '8818-0','8829-0','885-0','887-0','893-0','8934-0','895-0','8952-0','8967-0','90-0',
                                              '901-0','9019-0','902-0','904-0','9056-0','9057-0','9058-0','9059-0','9061-0','907-0',
                                              '912-0','914-0','92-0','920-0','922-0','935-0','937-0','939-0','94-0','941-0',
                                              '942-0','949-0','95-0','9501-0','9502-0','9503-0','953-0','958-0','96-0','969-0',
                                              '97-0','970-0','976-0','98-0','989-0','99-0','9919-0','101-0','1140-0','15-0','165-0',
                                              '208-0','230-0','341-0','377-0','385-0','414-0','420-0','476-0','48-0','505-0','510-0',
                                              '515-0','517-0','56-0','575-0','588-0','606-0','623-0','645-0','834-0','844-0','851-0',
                                              '855-0','883-0','921-0','938-0','1235-0','1287-0','1456-0','301-0','352-0','373-0',
                                              '441-0','447-0','529-0','590-0','705-0','748-0','805-0','810-0','952-0','983-0'),
                                       length=NA,tonnage=NA,power=NA,full_name = 'Old unknown vessel'))


#mfdb_import_tow_taxonomy(mdb,data.frame(name=c(1e5,4e5),latitude=c(64.69183,65.26833), longitude =c( -25.0890, -23.5125),
#                                        depth=c(201,54),length=c(4,4)))

mfdb_import_survey(mdb,
                   data_source = 'iceland-ldist',
                   ldist %>% filter(!(tow %in% c(1e5,4e5))) %>% mutate(vessel =ifelse(vessel=='-0',NA,vessel) ))


## age -- length data
aldist <-
  lesa_kvarnir(mar) %>% 
  rename(tow=synis_id) %>% 
  inner_join(tbl(mar,'species_key')) %>%
  right_join(tbl(mar,'stations') 
               #select(-length) 
               ) %>%
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
                   data_source = 'iceland-aldist',
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


gridcell.mapping <-
  plyr::ddply(reitmapping %>% filter(!grepl('_0', GRIDCELL)) 
              ,~DIVISION,function(x) head(x,1)) %>% 
  rename(areacell=GRIDCELL,division=DIVISION,subdivision=SUBDIVISION)
dbRemoveTable(mar,'port2sr')
port2division(0:999) %>% 
  left_join(gridcell.mapping) %>% 
  dbWriteTable(mar,'port2sr',.)
dbRemoveTable(mar,'port2sk')
port2division(0:999, skiki = TRUE) %>% 
  left_join(gridcell.mapping) %>% 
  dbWriteTable(mar,'port2sk',.)
dbRemoveTable(mar,'kfteg2sk')
kfteg2division<-
  data_frame(
  division = c(31, 32, 34, 52, 53, 55, 56, 59, 62, 63),
  kfteg =    c(58, NA, NA, 51, 53, 52, 55, 72, 54, 56) 
  #landings by snaefellsness default to division 31, but actually for 32 & 34 also
) %>% 
  left_join(gridcell.mapping)
dbWriteTable(mar,'kfteg2sk',kfteg2division)


## landing pre 1994 from fiskifélagið
dbRemoveTable(mar,'landed_catch_pre94') 
bind_rows(
  list.files('/net/hafkaldi/export/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = '^[0-9]+',full.names = TRUE) %>% 
    map(~read.table(.,skip=2,stringsAsFactors = FALSE,sep='\t')) %>% 
    bind_rows() %>% 
    rename_(.dots=stats::setNames(colnames(.),c('vf',	'skip',	'teg',	'ar',	'man',	'hofn',	'magn'))) %>% 
    mutate(magn=as.numeric(magn)),
  list.files('/net/hafkaldi/export/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = 'ready',full.names = TRUE) %>% 
    map(~read.table(.,skip=2,stringsAsFactors = FALSE,sep='\t')) %>% 
    bind_rows() %>% 
    rename_(.dots=stats::setNames(colnames(.),c(	'ar','hofn',	'man',	'vf',	'teg', 'magn'))),
  list.files('/net/hafkaldi/export/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = 'afli.[0-9]+$',full.names = TRUE) %>% 
    map(~read.table(.,skip=2,stringsAsFactors = FALSE,sep=';')) %>% 
    bind_rows()%>% 
    rename_(.dots=stats::setNames(colnames(.),c(	'ar','hofn',	'man',	'vf',	'teg', 'magn')))) %>%
  filter(!(ar==1991&is.na(skip))) %>% 
  mutate(veidisvaedi='I') %>% 
  rename(veidarfaeri=vf,skip_nr=skip,magn_oslaegt=magn,fteg=teg) %>% 
  dbWriteTable(mar,'landed_catch_pre94',.)

## catches 
landings_map <- 
  lods_oslaegt(mar) %>%
  left_join(tbl_mar(mar,'kvoti.skipasaga'), by='skip_nr') %>% 
  filter(l_dags < ur_gildi, l_dags > i_gildi) %>% 
  select(skip_nr,saga_nr,komunr,hofn) %>% 
  distinct()

landed_catch <- 
  lods_oslaegt(mar) %>%   
  left_join(landings_map) %>% 
  filter(ar > 1993) %>% 
  select(veidarfaeri, skip_nr, fteg, kfteg,
         ar, man, hofn, magn_oslaegt, 
         veidisvaedi, l_dags, saga_nr) %>% 
  dplyr::union_all(tbl(mar,'landed_catch_pre94') %>% 
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
  mutate(areacell = ifelse(species=='SHR', areacell.y, areacell.x),
         sampling_type='LND',
         gear = nvl(gear,'LLN')) %>% 
  select(weight_total=magn_oslaegt,sampling_type,areacell, vessel,species,year=ar,month=man,
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
         gear = nvl(gear, 'LLN')) %>% 
  select(weight_total=magn_oslaegt,sampling_type,areacell, vessel,species,year=ar,month=man,
         gear)

mfdb_import_survey(mdb,data_source = 'lods.foreign.landings',
                   foreign_landed_catch %>% collect(n=Inf) %>% as.data.frame())

url <- 'http://data.hafro.is/assmt/2017/'

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
  summarise(catch=sum(weight_total)/1000) %>% 
  arrange(desc(year)) %>% collect(n=Inf) -> tmp



ling_tusk_scalar <- 
  landingsByYear %>%
  filter(species %in% c('tusk','ling')) %>% 
  left_join(spitToDST2) %>% 
  select(Year:Total,species=shortname)%>% 
  rename(year=Year) %>% inner_join(tmp) %>%
  mutate(r=Iceland/catch) %>% 
  filter(year %in% 1993:2005) %>% 
  select(year,species,r)

landings <- 
  landed_catch %>% 
  collect(n=Inf) %>% 
  left_join(ling_tusk_scalar) %>% 
  mutate(weight_total = ifelse(is.na(r),weight_total,r*weight_total)) 

mfdb_import_survey(mdb,
                   data_source = 'commercial.landings',
                   data_in = landings %>% 
                     select(-r) %>% 
                     mutate(vessel =ifelse(vessel=='-0',NA,vessel)) %>% 
                     ## this is a hotfix due to importing problem and the landings are ~100 kg from these vessels
                     filter(!(vessel %in% c('1694-0','5000-0','5059-0',
                                            '5688-0','5721-0','8076-0','8091-0','9083-0')),
                            weight_total >0,
                            !is.na(weight_total)) %>% 
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
         weight_total = 1000*Others/12,
         gear = 'LLN', ## this needs serious consideration
         areacell = 2741) %>% ## just to have something
  mutate(shortname = NULL,
         Others = NULL,
         Total = NULL) %>% 
  as.data.frame()



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
         weight_total = 1000*(Total - Others)/12,
         gear = 'BMT', ## this needs serious consideration
         areacell = 2741) %>% ## just to have something
  mutate(shortname = NULL,
         Others = NULL,
         Total = NULL) %>% 
  as.data.frame()

mfdb_import_survey(mdb,
                   data_source = 'old.landings',
                   oldLandingsByMonth)

## statlant data, need to look further into this
load('/net/hafkaldi/export/home/haf/einarhj/r/Pakkar/landr/data/lices.rda')
tmp <- 
  lices %>%
  filter(as.numeric(sare)==5,tolower(div)=='a',
         grepl('Ling',species)|grepl('usk',species),
         country2 != 'Iceland',
         year > 1984 & ref=='1985_2012' & descr=='5_A'| year < 1985) %>%
  distinct() %>%  
  bind_rows(data_frame(year=2013,
                       species=c('Ling','USK'),
                       landings = c(1324, 1284))) %>% 
  left_join(expand.grid(year=1905:2015,month=1:12)) %>%
  mutate(species=ifelse(species=='Ling','LIN','USK'),
         weight_total=landings*1e3/12,
         sampling_type = 'FLND',
         gear = 'LLN', ## this needs serious consideration
         areacell = 2741) %>% 
  select(year,month,species,weight_total,gear,areacell,sampling_type) %>% 
  na.omit() %>% 
  as.data.frame() %>% 
  mfdb_import_survey(mdb,
                     data_source = 'statlant.foreign.landings',
                     .)

