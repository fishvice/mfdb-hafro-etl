## devtools::install_github('mareframe/mfdb',ref='6.x')
library(tidyverse)
library(tidyr)
library(mfdb)
library(mar)
library(purrr)
library(purrrlyr)
library(stringr)
library(ROracle)
library(dbplyr)


add_shrimp <- FALSE


## oracle connection 
mar <- connect_mar()


## Create connection to MFDB database, as the Icelandic case study

## Erase the old database 
mdb <- mfdb('Iceland', destroy_schema = TRUE)

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
  transmute(GRIDCELL = as.character(GRIDCELL), DIVISION, SUBDIVISION = as.character(SUBDIVISION), id, lat, lon, size) %>% 
  na.omit()

dbWriteTable(mar,'reitmapping',as.data.frame(reitmapping),overwrite=TRUE)

## import the area definitions into mfdb
mfdb_import_area(mdb, 
                 reitmapping %>% 
                   select(id,
                          name=GRIDCELL,
                          size) %>% 
                   as.data.frame()) 

## create area division (used for e.g. bootstrapping)
mfdb_import_division(mdb, reitmapping %>% split(.$SUBDIVISION) %>% map('GRIDCELL'))

## here some work on temperature would be nice to have instead of fixing the temperature
mfdb_import_temperature(mdb, 
                        expand.grid(year=1900:2100,
                                    month=1:12,
                                    areacell = reitmapping$GRIDCELL,
                                    temperature = 3))

## BEGIN hack
## species mapping
data.frame(tegund =  c(1:11, 19, 21, 22,
                       23, 25, 27, 30, 31, 48, 14,24 ),
           species = c('COD','HAD','POK','WHG','RED','LIN','BLI','USK',
                       'CAA','RNG','REB','GSS','HAL','GLH',
                       'PLE','WIT','DAB','HER','CAP','LUM','MON','LEM')) %>% 
  dbWriteTable(mar,'species_key',.,overwrite = TRUE)

## gear mapping
mapping <- 
  read_delim('inst/mapping.txt',delim=' ') %>% 
  inner_join(gear,by=c('gear'='id')) %>% 
  select(-c(gear,description,t_group)) %>% 
  rename(gear = name) %>% 
  mutate(gear = as.character(gear))

dbWriteTable(conn = mar, 
             value = mapping,
             name = 'gear_mapping',
             overwrite = TRUE)

## Set-up sampling types

mfdb_import_sampling_type(mdb, data.frame(
  id = 1:18,
  name = c('SEA', 'IGFS','AUT','SMN','LND','LOG','INS','ACU','FLND','OLND','CAA',
           'CAP','GRE','FAER','RED','RAC','LOBS','ADH'),
  description = c('Sea sampling', 'Icelandic ground fish survey',
                  'Icelandic autumn survey','Icelandic gillnet survey',
                  'Landings','Logbooks','Icelandic nephrop survey',
                  'Acoustic capelin survey','Foreign vessel landings','Old landings (pre 1981)',
                  'Old catch at age','Capelin data','Eastern Greenland autumn survey',
                  'Faeroese summer survey','Redfish survey','Redfish accoustic survey',
                  'Icelandic nephrops survey','Ad-hoc surveys')))

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
  mutate(saga_nr = nvl(saga_nr,0)) %>% 
  filter(synaflokkur %in% c(1,2,8,10,12,20,30,34,35,38)) %>% 
  mutate(sampling_type = ifelse(synaflokkur %in% c(1,2,8),'SEA',
                                ifelse(synaflokkur %in% c(10,12,20),'ADH',
                                       ifelse(synaflokkur == 30,'IGFS',
                                              ifelse(synaflokkur==35,'AUT',
                                                     ifelse(synaflokkur==38,'LOBS','SMN'))))),
         ## This is a March survey but Gadget stocks recruit after being predated, -> bump the timing to next quarter
         man = ifelse(synaflokkur == 30, 4,   
                      ifelse(synaflokkur == 35,10,man)),
         institute = 'MRI',
         vessel = concat(concat(skip,'-'),saga_nr)) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>% 
  select(synis_id,ar,man,lat=kastad_n_breidd,lon=kastad_v_lengd,lat1=hift_n_breidd,lon1=hift_v_lengd,
         gear,sampling_type,depth=dypi_kastad,vessel,reitur,smareitur) %>% 
  ## this part will be fixed in the db soon
#  mutate(lat=nvl(geoconvert1(lat),0),
#         lon=nvl(geoconvert1(lon),0)) %>% 
  mutate(areacell=as.character(10*reitur+nvl(smareitur,1))) %>% 
         ## this bit should also be fixed soon
#         lon1 =   nvl(geoconvert1(lon1),lon),
#         lat1 = nvl(geoconvert1(lat1),lat)) %>% 
  mutate(towlength = arcdist(lat,lon,lat1,lon1)) %>%
  select(-c(lat1,lon1,reitur,smareitur)) %>% 
  inner_join(tbl(mar,'reitmapping') %>% 
               select(areacell=GRIDCELL),
             by='areacell') %>% 
  rename(tow=synis_id) %>% 
  rename(year = ar) %>% 
  rename(month = man) %>% 
  rename(latitude =lat) %>% 
  rename(longitude = lon)

dbRemoveTable(mar,'stations')
stations %>% 
  compute(name='stations',temporary=FALSE,indexes = list('tow'))

## information on tows
tbl(mar,'stations') %>% 
  rename(name = tow) %>% 
  collect(n=Inf) %>% 
  as.data.frame() %>%
  #mutate(name = ifelse(name == 1e5, '100000',ifelse(name == 4e5,'400000',as.character(name)))) %>% 
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
dbRemoveTable(mar,'ldist')
lesa_lengdir(mar) %>%
  inner_join(tbl(mar,'species_key')) %>% 
  skala_med_toldum() %>% 
  rename(tow=synis_id) %>% 
  compute(name='ldist',temporary=FALSE)

ldist <- 
  tbl(mar,'ldist') %>% 
  right_join(tbl(mar,'stations') %>% 
               select(-towlength),
             by = 'tow') %>% 
  mutate(lengd = nvl(lengd,0), #ifelse(is.na(lengd), 0, lengd),
         fjoldi = nvl(fjoldi,0), #ifelse(is.na(fjoldi), 0, fjoldi),
         kyn = ifelse(kyn == 2,'F',ifelse(kyn ==1,'M','')),
         kynthroski = ifelse(tegund==9, 
                             ifelse(kynthroski > 2 & kyn == 'F',2,ifelse(kynthroski %in% c(1,2) & kyn == 'F',1,NA)), 
                             ifelse(kynthroski > 1,2,ifelse(kynthroski == 1,1,NA))),
         age = 0)%>%
  select(-c(r,tegund)) %>% 
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
                                              '97-0','970-0','976-0','98-0','989-0','99-0','9919-0', 
                                              '1235-0','1287-0','1456-0','301-0','341-0','352-0','373-0','414-0','441-0',
                                              '447-0','505-0','515-0','529-0','56-0','590-0','623-0','705-0','748-0',
                                              '805-0','810-0','952-0','983-0', '101-0','1140-0','15-0','165-0','208-0','230-0','377-0',
                                              '385-0','420-0','476-0','48-0','510-0','517-0','575-0','588-0','606-0','645-0','834-0',
                                              '844-0','851-0','855-0','883-0','921-0','938-0'),

                                       length=NA,tonnage=NA,power=NA,full_name = 'Old unknown vessel'))


#mfdb_import_tow_taxonomy(mdb,data.frame(name=c(1e5,4e5),latitude=c(64.69183,65.26833), longitude =c( -25.0890, -23.5125),
#                                        depth=c(201,54),length=c(4,4)))


mfdb_import_survey(mdb,
                   data_source = 'iceland-ldist',
                   ldist %>% mutate(vessel =ifelse(vessel=='-0',NA,vessel) ))


## age -- length data

aldist <-
  lesa_kvarnir(mar) %>% 
  rename(tow=synis_id) %>% 
  inner_join(tbl(mar,'species_key')) %>%
  right_join(tbl(mar,'stations') %>% 
               select(-towlength)) %>%
  mutate(lengd = nvl(lengd,0),
         count = 1,
         kyn = ifelse(kyn == 2,'F',ifelse(kyn ==1,'M',NA)),
         kynthroski = ifelse(tegund==9, 
                             ifelse(kynthroski > 2 & kyn == 'F',2,ifelse(kynthroski %in% c(1,2) & kyn == 'F',1,NA)), 
                             ifelse(kynthroski > 1,2,ifelse(kynthroski == 1,1,NA)))
          )%>%
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
dbRemoveTable(mar,'port2sr')
port2division(0:999) %>% 
  left_join(gridcell.mapping) %>% 
  dbWriteTable(mar,'port2sr',.)    

## landing pre 1994 from fiskifélagið

dbRemoveTable(mar,'landed_catch_pre94') 
bind_rows(
  list.files('/net/hafkaldi.hafro.is/export/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = '^[0-9]+',full.names = TRUE) %>% 
    map(~read.table(.,skip=2,stringsAsFactors = FALSE,sep='\t')) %>% 
    bind_rows() %>% 
    rename_(.dots=stats::setNames(colnames(.),c('vf',	'skip',	'teg',	'ar',	'man',	'hofn',	'magn'))) %>% 
    mutate(magn=as.numeric(magn)),
  list.files('/net/hafkaldi.hafro.is/export/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = 'ready',full.names = TRUE) %>% 
    map(~read.table(.,skip=2,stringsAsFactors = FALSE,sep='\t')) %>% 
    bind_rows() %>% 
    rename_(.dots=stats::setNames(colnames(.),c(	'ar','hofn',	'man',	'vf',	'teg', 'magn'))),
  list.files('/net/hafkaldi.hafro.is/export/u2/reikn/R/Pakkar/Logbooks/Olddata',pattern = 'afli.[0-9]+$',full.names = TRUE) %>% 
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
         gear = nvl(gear,'LLN')) %>% 
  select(weight_total=magn_oslaegt,sampling_type,areacell, vessel,species,year=ar,month=man,
         gear)



foreign_landed_catch <- 
  lods_oslaegt(mar) %>%   
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
load('/net/hafkaldi.hafro.is/export/home/haf/einarhj/r/Pakkar/landr/data/lices.rda')
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


fiskifelagid_landing_pre82 <- 
  tbl_mar(mar,'fiskifelagid.vigtarskra66_81') %>%
  mutate(l_dags = to_date(concat(artal,concat('.',manudur)),'yyyy.mm'),
         kfteg = 0,
         magn_oslaegt = reiknistudull * magn,
         veidisvaedi = 'I') %>% 
  mutate(timabil = if_else(to_number(to_char(l_dags, "MM")) < 9,
                           concat(to_number(to_char(l_dags, "YYYY")) -1, to_number(to_char(l_dags, "YYYY"))),
                           concat(to_number(to_char(l_dags, "YYYY")), to_number(to_char(l_dags, "YYYY")) + 1))) %>% 
  select(skip_nr,hofn=vinnsluhofn,
         komunr=radlykill,l_dags,gerd=skipsgerd,
         fteg,kfteg,magn_oslaegt,veidarfaeri,
         ar = artal,man = manudur,timabil) %>% 
  left_join(landings_map) %>% 
  left_join(tbl_mar(mar,'kvoti.skipasaga'), by=c('skip_nr','saga_nr')) %>% 
  mutate(vessel = concat(concat(nvl(skip_nr,''),'-'),nvl(saga_nr,0)),
         flokkur = nvl(flokkur,0)) %>% 
  #select(skip_nr,hofn,l_dags,saga_nr,gerd,fteg,kfteg,veidisvaedi,stada,veidarfaeri,magn_oslaegt, i_gildi,flokkur,ar,man) %>% 
  #filter(veidisvaedi == 'I',flokkur != -4) %>% 
  left_join(tbl(mar,'gear_mapping'),by='veidarfaeri') %>%
  inner_join(tbl(mar,'species_key'),by=c('fteg'='tegund')) %>%
  left_join(tbl(mar,'port2sr'),by='hofn') %>%
  mutate(sampling_type='LND',
         gear = nvl(gear,'LLN')) %>% 
  select(weight_total=magn_oslaegt,sampling_type,areacell, vessel,species,year=ar,month=man,
         gear) %>% 
  collect(n=Inf)
  

mfdb_import_survey(mdb,
                   data_source = 'fiskifelagid_pre82.landings',
                   data_in = fiskifelagid_landing_pre82 %>% 
                     select(-vessel) %>% 
                            as.data.frame())

mfdb_import_vessel_taxonomy(mdb,
                            data.frame(name=c('100000-0', '100044-0', '100084-0', '100088-0', '100101-0', '100102-0', '100103-0', 
                                              '100104-0', '100127-0', '100129-0', '100133-0', '100144-0', '100199-0', '100236-0', 
                                              '100237-0', '100252-0', '100338-0', '100341-0', '100362-0', '100378-0', '100383-0', 
                                              '100388-0', '100402-0', '100410-0', '100461-0', '100481-0', '100486-0', '100501-0', 
                                              '100617-0', '100701-0', '100702-0', '100706-0', '1008-0', '100904-0', '100971-0',                                               
                                              '100989-0', '101013-0', '101042-0', '101095-0', '101101-0', '101104-0', '101105-0', 
                                              '101106-0', '101109-0', '101112-0', '101115-0', '101265-0', '101300-0', '101301-0',
                                              '101303-0'), 
                                       length=NA,tonnage=NA,power=NA,full_name = 'Old unknown vessel'))

mfdb_import_vessel_taxonomy(mdb,
                            data.frame(name=c('101312-0', '101315-0', '101316-0', '101321-0', '101325-0', 
                                              '101328-0', '101329-0', '101330-0', '101335-0', '101345-0', 
                                              '101502-0', '101701-0', '101702-0', '101716-0', '101719-0', 
                                              '101720-0', '101724-0', '101726-0', '101730-0', '101915-0', 
                                              '101917-0', '101921-0', '101940-0', '101950-0', '101958-0',
                                              '101968-0', '101992-0', '102102-0', '102105-0', '102107-0',
                                              '102115-0', '102116-0', '102123-0', '102134-0', '102146-0',
                                              '102147-0', '102160-0', '102165-0', '102168-0', '102181-0', 
                                              '1022-0', '102306-0', '102316-0', '102318-0', '102320-0',
                                              '1024-0', '102501-0', '102506-0', '102515-0', '102517-0'), 
                                       length=NA, tonnage=NA, power=NA, full_name = 'Old unknown vessel'))



mfdb_import_vessel_taxonomy(mdb,
                            data.frame(name=c('101312-0', '101315-0', '101316-0', '101321-0', '101325-0', 
                                              '101328-0', '101329-0', '101330-0', '101335-0', '101345-0', 
                                              '101502-0', '101701-0', '101702-0', '101716-0', '101719-0', 
                                              '101720-0', '101724-0', '101726-0', '101730-0', '101915-0', 
                                              '101917-0', '101921-0', '101940-0', '101950-0', '101958-0',
                                              '101968-0', '101992-0', '102102-0', '102105-0', '102107-0',
                                              '102115-0', '102116-0', '102123-0', '102134-0', '102146-0',
                                              '102147-0', '102160-0', '102165-0', '102168-0', '102181-0', 
                                              '1022-0', '102306-0', '102316-0', '102318-0', '102320-0',
                                              '1024-0', '102501-0', '102506-0', '102515-0', '102517-0'), 
                                       length=NA, tonnage=NA, power=NA, full_name = 'Old unknown vessel'))


if(add_shrimp){try(source('R/initdb_add_shrimp.R'))}

