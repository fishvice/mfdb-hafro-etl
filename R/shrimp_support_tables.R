

#inclusion criteria to be implemented at modeling stage:
#synaflokkur == 37
#skiki %in% c(31,32,34,52,53,55,56,59,62,63)
#!is.na(tognumer)
#!is.na(synis_id)
#remove bad_synis_id


####### flatarmal ###########  used to define reitmapping in initdb.R 
flat1 <- c(34.7,36.2, 44.4, 30.7,30.7); flat1 <- as.data.frame(cbind(fjardarreitur=1:5, skiki=52,flat1)) 
names(flat1) <- replace(names(flat1), names(flat1) == "flat1", "flat") #arnarfj?r?ur 1:5
flat2 <- c(38.29,20.08,94.95,85.47,18.03,5.67,30.37,3.09)
flat2 <- as.data.frame(cbind(fjardarreitur=1:8, skiki=55, flat2)) 
names(flat2) <- replace(names(flat2), names(flat2) == "flat2", "flat") # h?nafl 1:8
flat3 <- c(81.8, 85.6,81.7,21,53.7,22.2); flat3 <- as.data.frame(cbind(fjardarreitur=c(1,2,3,4,6,8), skiki=53, flat3))  # fjardarreitir: 1,2,3,4,6,8 ?safj
names(flat3) <- replace(names(flat3), names(flat3) == "flat3", "flat") 
flat4 <- c(92.62,70.69, 9.95, 20.78); flat4 <- as.data.frame(cbind(fjardarreitur=1:4, skiki=62, flat4)) # skagafj 1:4
names(flat4) <- replace(names(flat4), names(flat4) == "flat4", "flat") 
flat5 <- c(63.8,157.1); flat5 <- as.data.frame(cbind(fjardarreitur=1:2, skiki=63, flat5))
names(flat5) <- replace(names(flat5), names(flat5) == "flat5", "flat") # skjalf 1:2
flat6 <- c(46.81,9.79,40.76,42.15,200.6,625.21,165.32,169.72,132.28)
flat6 <- as.data.frame(cbind(fjardarreitur=1:9, skiki=c(34,34,34,34,32,32,31,31,31), flat6)) 
names(flat6) <- replace(names(flat6), names(flat6) == "flat6", "flat") # fjar?arreitir 1-9 sn?f
flat7 <- c(25.96,77, 110.95, 156.11); flat7 <- as.data.frame(cbind(fjardarreitur=1:4, skiki=56, flat7)) # axarfj 1:4
names(flat7) <- replace(names(flat7), names(flat7) == "flat7", "flat") 
flat8 <- c(161,246,68); flat8 <- as.data.frame(cbind(fjardarreitur=1:3, skiki=59,flat8))
names(flat8) <- replace(names(flat8), names(flat8) == "flat8", "flat")

flatarmal <- rbind(flat1, flat2, flat3, flat4, flat5, flat6, flat7,flat8) %>%
  transmute(GRIDCELL = paste(skiki, fjardarreitur, sep = "_"), SUBDIVISION = paste(skiki, fjardarreitur, sep = "_"), DIVISION = skiki, size = flat) 


#### fjarlaegja faerslur ########
bad_synis_id<-c(17996 # eitthvad aukatog - raekjuleit
                ,203714 # endurtekning a tognr 1 ... toglengd/togtimi skrytid
                #veidarfaeri 30 i D12-92
                ,57555 
                ,57556 
                ,57557 
                ,57558 
                ,57559 
                ,57560 
                ,57561
                #### til ad visitolur reiknist eins - taka ut tognumer 8 og 11  skiki 53 ar 2012:  #####
                # tog i fullri konnun en thad var bara skert konnun thetta arid + alls konar aukatog (thetta eru ? raun aukatog)
                ,389863
                ,389866
)


###### skiki #####
corrected_skiki<-data.frame(matrix(c(265871, 55 
                                     , 402691, 59
                                     , 231367,  32
                                     , 210459,  32
                                     , 189400,  34
                                     , 91523,  34
                                     , 81517,  34
                                     , 73759,  34
                                     , 57726,  34
                                     , 49306,  34
                                     , 322156,  32
                                     , 322157,  32
                                     , 52720,  34
                                     , 63774,  34
                                     , 42446,  60
                                     , 348652,  32
                                     , 348651,  32
                                     , 348650,  32
                                     , 45200,  60), ncol = 2, byrow = TRUE))
names(corrected_skiki) <- c('synis_id', 'skiki.fx')

dbRemoveTable(mar,'corrected_skiki')

dbWriteTable(mar, 'corrected_skiki', corrected_skiki)


#####----REASSIGN SYNAFLOKKUR TO SUPPORT DATA SUBSETTING (with model definition)----#####

print("Warning! Vector leidangur_full needs to be updated every year to include most recent survey name.")

#Define support tables
######### Eldey ##########
eldey<-c('D6-88' , 'D9-89' , 'D2-90' ,
         'D6-91' , 'D6-92' , 'D6-93' ,
         'D7-94' , 'D6-95' , 'D6-96' ,
         'D12-97' , 'D13-98' , 'D10-99' ,
         'D4-2000' , 'D5-2001' , 'D4-2002' ,
         'D5-2004' , 'GR1-2010', 'D3-2013' ,
         'D5-2014' , 'D3-2015' , 'B10-2016')
########### Snaefellsnes ############
snaefellsness<-c('D6-88' , 'D3-91' , 'D6-92' ,
                 'D6-93' , 'L14-90' , 
                 'D5-93' , 'D5-94' , 'D3-95' ,
                 'D4-95' , 'D3-96' , 'D6-96' ,
                 'D9-97' , 'D12-97' , 'D11-98' ,
                 'D7-99' , 'KF1-2000' , 'D3-2001' ,
                 'D4-2002' , 'D3-2003' , 'D3-2004' ,
                 'NYT3-2005' , 'SNAE-2006' , 'SNAE1-2007' ,
                 'D4-2008' ,
                 'D3-2009' , 'D5-2010' , 'D4-2011' ,
                 'D3-2012' , 'D1-2013' , 'D3-2014' ,
                 'D1-2015' , 'B6-2016')
########## Arnarfjordur #############
arnar.h <- c('D11-88' ,  'D11-89' ,  'D8-90' ,
             'D12-91' ,  'D12-92' ,  'D11-93' , 
             'D13.B-94' ,  'D12-95' ,  'D11-96' ,
             'D18-97' ,  'D21-98' ,  'D19-99' ,
             'D9-2000' ,  'D11-2001' ,  'D9-2002' ,
             'D10-2003' ,  'NYT7-2004' ,  'D7-2005' ,
             'D7-2006' ,  'D7-2007' ,  'D12-2008' ,
             'D13-2009' ,  'D12-2010' ,  'D8-2011' ,
             'D9-2012' ,  'D7-2013' ,  'D7-2014' ,
             'D4-2015' ,  'B15-2016' , 'GU1-2017')
arnar.v <- c('L4-88' , 'L3-89' , 'L7-90' ,
             'VER-91' , 'LISU1-92' , 'D1-93' ,
             'D2-94' , 'D1-95' , 'D1-96' ,
             'D2-97' , 'D3-98' , 'D1-99' ,
             'D1-2000' , 'D1-2001' , 'ISJ1-2002' ,
             'ISJ1-2003' , 'D1-2004' , 'ISJ1-2005' ,
             'ISJ2-2006' , 'ISJ1-2008' , 'D2-2014' ,
             'ARN1-2015')
########## Isafjardardjup ###################
isa.h <- c('D11-88' , 'D11-89' , 'D8-90' ,
           'D12-91' , 'D12-92' , 'D11-93' ,
           'D13.A-94', 'D12-95' , 'D11-96' ,
           'D18-97' , 'D21-98' , 'D19-99' ,
           'D9-2000' , 'D11-2001' , 'D9-2002' ,
           'D10-2003' , 'NYT7-2004' , 'D6-2005' ,
           'D4-2006' , 'D6-2007' , 'D11-2008' ,
           'D12-2009' , 'D14-2010' , 'D8-2011' ,
           'D9-2012' , 'D5-2013' , 'D7-2014' ,
           'D4-2015' , 'B15-2016','GU1-2017', 'B13-2018')
isa.v <- c('L4-88' , 'L3-89' , 'L3-90' ,
           'LISU1-92' , 'D1-93' ,
           'D2-94' , 'D1-95' , 'D1-96' ,
           'D2-97' , 'D3-98' , 'D1-99' ,
           'D1-2000' , 'D1-2001' , 'ISJ2-2002' ,
           'ISJ3-2002' , 'ISJ2-2003' , 'D1-2004' ,
           'ISJ1-2013' ,
           'D2-2014' , 'ISJ2-2016' , 'ISJ3-2016',
           'VER-91')
###########Skjalfandi ##############
skjalfandi <- c('D10-89' , 'D9-90' , 'D13-91' ,
                'D11-92' , 'D12-93' , 'D14.A-94' ,
                'D11-95' , 'D10-96' , 'D17-97' ,
                'D20-98' , 'D18-99' , 'D8-2000' ,
                'D10-2001' , 'NYT13-2002' , 'D12-2003' ,
                'D5-2005' , 'D3-2007' , 'D8-2008' ,
                'D9-2009' , 'D12-2010' , 'D8-2011' ,
                'D9-2012', 'D5-2013' , 'D7-2014' ,
                'D5-2015' , 'B15-2016',
                'L6-90' , 'LGB1-92' , 'HUS1-93',
                'HUS1-94' , 'HUS1-95' , 'HUS2-96' ,
                'HUS1-97' , 'HUS1-98' , 'HUS1-99' ,
                'ASJ1-2000' , 'NYT3-2001' , 'NYT3-2002' ,
                'NYT4-2003' , 'NYT3-2004' , 'ASJ1-2014' ,
                'NYT1-2016')
############## Axarfj?r?ur ##################
axa.h <- c('D10-89' , 'D9-90' , 'D13-91' , 'D11-92' ,
           'OX2-93' , 'D14.A-94' , 'D11-95' ,
           'D10-96' , 'D17-97' , 'D20-98' ,
           'D18-99' , 'D8-2000' , 'D10-2001' ,
           'NYT14-2002' , 'D12-2003' , 'D5-2005' ,
           'D6-2006' , 'D2-2007' , 'D7-2008' ,
           'D10-2009' , 'D12-2010' , 'D8-2011' ,
           'D9-2012', 'D5-2013' , 'D7-2014' ,
           'D5-2015' , 'B15-2016')
axa.v <- c('L1-90' , 'OX1-93' , 'OX1-94' ,
           'OX1-95' , 'AX1-96' , 'ÖX1-97' ,
           'ÖX1-98' , 'AX1-99' , 'NYT2-2000' ,
           'NYT2-2001' , 'NYT2-2002' , 'NYT3-2003' ,
           'NYT2-2004')
############## Skagafj?r?ur #########
skagafjordur <- c('D11-89' , 'L31-90' , 'LSK2-91' ,
                  'D11-92' , 'SK4-93' , 'D14.A-94' ,
                  'D11-95' , 'D10-96' , 'D17-97' ,
                  'D20-98' , 'D18-99' , 'D8-2000' ,
                  'D10-2001' , 'NYT15-2002' , 'D12-2003' ,
                  'D4-2005' , 'D4-2007' , 'D9-2008' ,
                  'D11-2009' , 'D12-2010' , 'D8-2011' ,
                  'D9-2012', 'D5-2013' , 'D7-2014' ,
                  'D5-2015' , 'B15-2016',
                  'HU1-89' , 'L4-90' , 'LL2-91' ,
                  'LSK1-92' , 'SK1-93' , 'SK1-94' ,
                  'SK1-95' , 'SK1-96' , 'SK1-97' ,
                  'SK1-98' , 'SK1-99' , 'ASJ2-2000' ,
                  'NYT4-2002' , 'NYT5-2003' , 'NYT4-2004')
########### H?nafl?i #############
hunafloi <- c('D11-88' , 'D11-89' , 'D9-90' ,
              'D13-91' , 'D11-92' , 'D12-93' ,
              'D14.A-94' , 'D11-95' , 'D10-96' ,
              'D17-97' , 'D20-98' , "D18-99" ,
              'D8-2000' , 'D10-2001' , 'NYT12-2002' ,
              'D12-2003' , 'D4-2005' , 'D5-2006' ,
              'D5-2007' , 'D10-2008' , 'D8-2009' ,
              'D12-2010' , 'D8-2011' , 'D9-2012', 
              'D5-2013' , 'D7-2014' , 'D5-2015' ,
              'B15-2016',
              'L2-88' , 'L2-89' , 'L5-90' ,
              'LHU1-91' , 'LHU1-92' , 'HUN1-93' ,
              'HUN1-94' , 'HUN1-95' , 'HUN1-96' ,
              'HUN1-97' , 'HUN1-98', 'HUN1-99' ,
              'NYT3-2000' , 'NYT4-2001' , "NYT5-2002" ,
              'NYT2-2003' , 'NYT5-2004')
leidangur_full<-unique(c(eldey, snaefellsness, arnar.h, arnar.v, isa.h, isa.v, skjalfandi, 
                         axa.h, axa.v, skagafjordur, hunafloi))

#####----ORIGINAL MODIFICATIONS----#####
#reassign synaflokkur based on leidangur 
#exclusion_criteria <- c(#(skiki == 52 & leidangur %in% arnar.h & synaflokkur != 37) | #was not implemented because synaflokkur != 37 repeats criteria below during modeling stage
#(skiki == 52 & leidangur %in% arnar.v & man = 12) | #was not implemented here because synnaflokkur are all 2
#(skiki == 53 & leidangur %in% isa.h & veidarfaeri == 30) | #IMPLEMENTED HERE
#(skiki == 53 & leidangur %in% 'VER-91' & is.na(tognumer)) | #Inga checking, not implemented because repeats criteria below during modeling stage
#(skiki == 56 & leidangur %in% axa.h & ar <= 1989) | #IMPLEMENTED HERE
#!(skiki %in% c(31,32,34,52,53,55,56,59,62,63)) | #outside study area - will be implemented at modeling stage
#(skiki == 63 & tognumer >= 12) | #these are extra tows #IMPLEMENTED HERE
#(skiki == 55 & tognumer >= 47) | #these are extra tows #IMPLEMENTED HERE
#(is.na(tognumer)) | #will be implemented at modeling stage
#(tognumer >= 55 | tognumer == 0) | #extra or outside fjardareiturs #IMPLEMENTED HERE
#(is.na(synis_id))) #will be implemented at modeling stage

#more_corrected_synaflokkur_criteria <- "synaflokkur == 37 & ((skiki == 53 & leidangur %in% isa.h & veidarfaeri == 30) | (skiki == 56 & leidangur %in% axa.h & ar <= 1989) | (skiki == 63 & tognumer >= 12 & synaflokkur == 37) | (skiki == 55 & tognumer >= 47 & synaflokkur == 37) |  (tognumer >= 55 | tognumer == 0))" #was 37, also 20 or many other synnaflokkur
#use eval(parse(text = more_corrected synaflokkur_criteria)) to evaluate

#####  hiti  ####
corrected_botnhiti<-data.frame(matrix(c(389890, 5.3
                                        , 245607, 7.086
                                        , 245625, 9.956
                                        , 245629, 4.487
                                        , 294744, 5.010
                                        , 294745, 5.640
                                        , 294746, 8.066
                                        , 294747, 8.117
                                        , 294748, 8.103
                                        , 294749, 7.425
                                        , 294750, 6.869
                                        , 294751, 5.811
                                        , 294752, 6.498
                                        , 294753, 5.249
                                        , 294754, 6.403
                                        , 294755, 7.043
                                        , 294756, 6.596
                                        , 294757, 6.021
                                        , 294758, 4.993
                                        , 294759, 5.441
                                        , 294760, 5.146
                                        , 294761, 5.867
                                        , 294762, 6.251
                                        , 294763, 6.624
                                        , 294764, 5.608
                                        , 294765, 5.687
                                        , 420389, 7.8
                                        , 420392, 3.8
                                        , 420393, 3.8
                                        , 435450, 5.85), ncol = 2, byrow = TRUE))
names(corrected_botnhiti) <- c('synis_id', 'botnhiti.fx')

dbRemoveTable(mar,'corrected_botnhiti')

dbWriteTable(mar, 'corrected_botnhiti', corrected_botnhiti)

corrected_yfirbordshiti<-data.frame(matrix(c(55703, 6.3), ncol = 2, byrow = TRUE)) #arn
names(corrected_yfirbordshiti) <- c('synis_id', 'yfirbordshiti.fx')

dbRemoveTable(mar,'corrected_yfirbordshiti')

dbWriteTable(mar, 'corrected_yfirbordshiti', corrected_yfirbordshiti)

###### breidd #######

corrected_breidd <- data.frame(matrix( c(65984, 654284), ncol = 2, byrow = TRUE)) # tok stadsetningu ur handbok!
names(corrected_breidd) <- c('synis_id', "kastad_n_breidd.fx")

dbRemoveTable(mar,'corrected_breidd')

dbWriteTable(mar, 'corrected_breidd', corrected_breidd)

####### tognumer ########
####### NAs switched to -1#####
corrected_tognumer_init<-data.frame(matrix(c(40419, 37
                                             ,40364, 1
                                             ,42609, 13
                                             ,42482, 1
                                             ,45954, 4
                                             ,45935, 44
                                             ,45925, 17
                                             ,49378, 21
                                             ,49545, 6
                                             ,52852, 6
                                             ,52870, 15
                                             ,60507, 13
                                             ,83559, 12
                                             ,83565, 39
                                             ,154598, 50
                                             ,172962, 49
                                             ,197861, 20
                                             ,46175, 9
                                             ,46162, 2
                                             ,46160, 1
                                             ,229690, 19
                                             ,52842, 10
                                             ,49547, -1
                                             ,49546, 3
                                             ,197834, -1
                                             ,42106, 3
                                             ,163481, -1
                                             ,163553, -1
                                             ,163558, -1
                                             ,268466, -1
                                             ,45874, 21
                                             ,49709, 22 # gaeti verid rangt?
                                             ,52308, 11
                                             ,71294, 55
                                             ,167325, 1
                                             ,64461, 3
                                             ,49878, -1
                                             ,42465, -1
                                             ,42466, -1
                                             ,42468, -1
                                             ,42469, -1
                                             ,42470, -1
                                             ,42467, -1
                                             ,52723, 2
                                             ,294707, 5
                                             ,134453, 22
                                             ,84373, 7 # tognumerin hafa ruglast D11-96
                                             ,84374, 6 # tognumerin hafa ruglast D11-96
                                             # D2-92 Eldey
                                             ,45728, 10
                                             ,45730, 8
                                             ,45731, 7
                                             ,45732, 6
                                             ,45733, 5
                                             ,45734, 4
                                             ,45735, 3
                                             ,45736, 2
                                             ,45737, 1
                                             # snaef L14-90
                                             ,45200, 9
                                             ,45199, 10
                                             ,45198, 11
                                             ,45197, 12
                                             ,45196, 13
                                             ,45195, 15
                                             ,45208, 1
                                             ,45207, 2
                                             ,45206, 3
                                             ,45205, 4
                                             ,45204, 5
                                             ,45203, 6
                                             ,45202, 7
                                             ,45201, 8
                                             ,45193, 17
                                             ,45194, 16), ncol = 2, byrow = TRUE))
corrected_tognumer_init <- data.frame(corrected_tognumer_init, order = 1:dim(corrected_tognumer_init)[1])
names(corrected_tognumer_init) <- c('synis_id', 'tognumer.fx', 'order')

#no repeats
corrected_tognumer <- corrected_tognumer_init %>% select(-c(order))

dbRemoveTable(mar,'corrected_tognumer')

dbWriteTable(mar, 'corrected_tognumer', corrected_tognumer)

##### togtimi ############
corrected_togtimi_init <-data.frame(matrix(c(40410, 45
                                             ,42515, 60
                                             ,42597, 60
                                             ,42614, 60
                                             ,42451, 60
                                             ,45886, 30
                                             ,57547, 39
                                             ,52971, 30
                                             ,60625, 60
                                             ,60499, 30
                                             ,65985, 30
                                             ,65988, 45
                                             ,91592, 85
                                             ,237359, 78
                                             ,82777, 30
                                             ,49541, 30
                                             ,46174, 40
                                             ,46174, 40
                                             ,57129, 60
                                             ,57128, 60
                                             ,63166, 65
                                             ,63163, 60
                                             ,68026, 63
                                             ,86351, 60
                                             ,86347, 60
                                             ,86312, 60
                                             ,120347, 60
                                             ,197895, 60
                                             ,72130, 45
                                             ,42092, 60
                                             ,47219, 30
                                             ,57162, 50
                                             ,57166, 60
                                             ,63206, 40
                                             ,80946, 60
                                             ,163494, 40
                                             ,180076, 40
                                             ,180077, 40
                                             ,59829, 60
                                             ,49409, 50
                                             ,49417, 60
                                             ,52320, 56
                                             ,57372, 55
                                             ,73786, 60
                                             ,83565, 50
                                             ,83764, 53
                                             ,59892, 60
                                             ,59885, 60
                                             ,57547, 39
                                             ,57484, 45
                                             ,42520, 40
                                             ,42515, 60
                                             ,42597, 60
                                             ,42614, 60
                                             ,40410, 45
                                             ,45228, 60
                                             ,48837, 60
                                             ,48838, 60
                                             ,49043, 60
                                             ,52281, 50
                                             ,52299, 60
                                             ,52303, 60
                                             ,73830, 46
                                             ,231379, 81
                                             ,73579, 90
                                             ,73584, 90
                                             ,64601, 90
                                             ,52722, 90
                                             ,52708, 90
                                             ,52710, 90
                                             ,86466, 58
                                             ,48837, 50
                                             ,52281, 50
                                             ,49409, 60
                                             ,57372, 60
                                             ,49417, 60
                                             ,45228, 60
                                             ,42520, 40
                                             ,49348, 29
                                             ,172890, 52
                                             ,37559, 60
                                             ,72166, 60
                                             ,65893, 45
                                             ,49717, 60
                                             ,73486, 60
                                             ,73896, 88), ncol = 2, byrow = TRUE))
corrected_togtimi_init<-data.frame(corrected_togtimi_init, order = 1:dim(corrected_togtimi_init)[1])
names(corrected_togtimi_init) <- c('synis_id', 'togtimi.fx', 'order')

repeats <-
  corrected_togtimi_init %>% 
  distinct(synis_id, togtimi.fx,.keep_all = TRUE) %>% 
  group_by(synis_id) %>% 
  filter(n()>1) %>% 
  arrange(synis_id, order)

corrected_togtimi <-
  corrected_togtimi_init %>% 
  distinct(synis_id, togtimi.fx,.keep_all = TRUE) %>% 
  filter( !(order %in% repeats$order[seq(2,length(repeats$order),2)]) ) %>% 
  select(-c(order))

dbRemoveTable(mar,'corrected_togtimi')

dbWriteTable(mar, 'corrected_togtimi', corrected_togtimi)

########## toglengd #########

corrected_toglengd_init <- data.frame(matrix(c(154108, 2
                                               ,49537, 1
                                               ,57130, 1.8
                                               ,57132, 2.2
                                               ,57136, 2
                                               ,57142, 1.5
                                               ,57138, 1.5
                                               ,57137, 1
                                               ,63170, 1
                                               ,120347, 2
                                               ,161097, 1.7
                                               ,161096, 1.8
                                               ,60489, 2
                                               ,52555, 2
                                               ,52560, 2
                                               ,52561, 2
                                               ,52562, 2
                                               ,52557, 2
                                               ,52558, 2
                                               ,52559, 2
                                               ,52563, 2
                                               ,52564, 2
                                               ,57116, 2
                                               ,57120, 1.5
                                               ,57121, 2
                                               ,57122, 1.5
                                               ,57123, 2
                                               ,63153, 1.5
                                               ,63154, 1
                                               ,86379, 1.8
                                               ,142242, 1.8
                                               ,161160, 2
                                               ,72136, 2
                                               ,65978, 1
                                               ,49337, 1.5
                                               ,49345, 2
                                               ,49314, 2
                                               ,45885, 1.5
                                               ,45920, 2
                                               ,42424, 1
                                               ,46822, 2
                                               ,46627, 2
                                               ,46821, 2
                                               ,46626, 2
                                               ,46624, 1.5
                                               ,46623, 1.5
                                               ,46625, 2
                                               ,46622, 2.3
                                               ,46816, 2
                                               ,46621, 2
                                               ,46817, 2
                                               ,46818, 2
                                               ,46820, 2
                                               ,46819, 2
                                               ,46815, 2
                                               ,46811, 2
                                               ,46620, 2
                                               ,46814, 2
                                               ,46812, 2
                                               ,46619, 1.7
                                               ,46617, 2
                                               ,46813, 2
                                               ,46810, 2
                                               ,46618, 1.4
                                               ,46808, 2
                                               ,46809, 2
                                               ,46807, 2
                                               ,46806, 2
                                               ,46610, 1.5
                                               ,46611, 1.5
                                               ,46612, 1.3
                                               ,46613, 2
                                               ,46615, 1.3
                                               ,46614, 1.4
                                               ,46616, 1.1
                                               ,52621, 1.2
                                               ,52622, 2
                                               ,52633, 2
                                               ,52643, 2
                                               ,52648, 2
                                               ,52651, 2
                                               ,52653, 2
                                               ,52656, 2
                                               ,52658, 1.7
                                               ,52660, 2
                                               ,52662, 2
                                               ,52663, 2
                                               ,52666, 2
                                               ,52667, 2
                                               ,52668, 2
                                               ,52669, 2
                                               ,52670, 2
                                               ,52671, 2
                                               ,52672, 2
                                               ,52674, 2
                                               ,52677, 1
                                               ,52678, 1.6
                                               ,52680, 1.8
                                               ,52681, 1
                                               ,52683, 2
                                               ,52684, 2
                                               ,52685, 1.7
                                               ,52686, 2
                                               ,52640, 2
                                               ,57176, 2
                                               ,57187, 1
                                               ,57188, 2
                                               ,57196, 1.5
                                               ,63176, 2
                                               ,63177, 2
                                               ,63178, 2
                                               ,63182, 2
                                               ,63183, 2
                                               ,63185, 2
                                               ,63189, 2
                                               ,63190, 2
                                               ,63191, 2
                                               ,63193, 2
                                               ,63194, 2
                                               ,63197, 2
                                               ,63200, 2
                                               ,63206, 1.5
                                               ,63207, 2
                                               ,63208, 2
                                               ,63209, 1.7
                                               ,63211, 1.8
                                               ,63214, 1.5
                                               ,63215, 1.1
                                               ,63216, 1.3
                                               ,71288, 1
                                               ,86207, 1.5
                                               ,100782, 2
                                               ,202813, 2
                                               ,202818, 1.9
                                               ,72218, 2
                                               ,72224, 1.1
                                               ,72222, 1.8
                                               ,72221, 1.3
                                               ,72219, 1.5
                                               ,72226, 1.7
                                               ,72227, 1.6
                                               ,72229, 2.2
                                               ,72232, 2
                                               ,65750, 2
                                               ,65749, 2.2
                                               ,65748, 2.3
                                               ,65747, 2.3
                                               ,65740, 1.3
                                               ,65741, 2.1
                                               ,65742, 1.5
                                               ,65743, 1.5
                                               ,65744, 2
                                               ,65745, 1.3
                                               ,65739, 2
                                               ,65738, 1.5
                                               ,65737, 1.5
                                               ,65736, 1.7
                                               ,65735, 2
                                               ,65734, 2.1
                                               ,65730, 2
                                               ,65729, 2
                                               ,65733, 2.1
                                               ,65728, 2
                                               ,65727, 2.2
                                               ,65726, 1.8
                                               ,59835, 2
                                               ,59820, 1.8
                                               ,55691, 2
                                               ,55702, 1.3
                                               ,55695, 2
                                               ,55694, 1.6
                                               ,55693, 1.8
                                               ,55703, 1.5
                                               ,55700, 2
                                               ,55697, 2
                                               ,55704, 1.6
                                               ,55705, 2
                                               ,55708, 2
                                               ,49418, 2
                                               ,49417, 2
                                               ,45872, 2.4
                                               ,45871, 2.5
                                               ,45870, 2.4
                                               ,45749, 2.7
                                               ,45755, 1.7
                                               ,45754, 2.2
                                               ,45753, 2
                                               ,45752, 2.2
                                               ,45751, 2.3
                                               ,45750, 1.8
                                               ,45761, 2
                                               ,45759, 1.8
                                               ,45760, 1.7
                                               ,45758, 1.9
                                               ,45757, 2.4
                                               ,45756, 2.4
                                               ,45748, 2.1
                                               ,45869, 2.5
                                               ,45875, 2.4
                                               ,45868, 1.8
                                               ,45874, 2.4
                                               ,45873, 2.1
                                               ,42594, 2
                                               ,42593, 2.2
                                               ,42592, 2.2
                                               ,42591, 2.2
                                               ,42585, 2.2
                                               ,42586, 2
                                               ,42587, 2
                                               ,42588, 2
                                               ,42589, 2
                                               ,42590, 1.6
                                               ,42582, 2.2
                                               ,42581, 1.3
                                               ,42580, 1.4
                                               ,42579, 2
                                               ,42573, 2
                                               ,42583, 2
                                               ,42584, 1.6
                                               ,42575, 2.3
                                               ,42574, 2.4
                                               ,42576, 1.9
                                               ,42577, 2
                                               ,42578, 1.6
                                               ,161212, 2
                                               ,37943, 2.3
                                               ,37942, 2.2
                                               ,37941, 2.4
                                               ,37933, 2.1
                                               ,37936, 1.9
                                               ,37937, 1.7
                                               ,161215, 1.3
                                               ,37938, 2.2
                                               ,37939, 1.5
                                               ,37928, 2
                                               ,37931, 1.3
                                               ,37932, 1.9
                                               ,37930, 1.7
                                               ,37929, 1.5
                                               ,37935, 2.4
                                               ,37940, 2
                                               ,37927, 2.2
                                               ,37934, 2.5
                                               ,37926, 1.9
                                               ,161217, 1.8
                                               ,43428, 1.9
                                               ,43426, 1.8
                                               ,43425, 1.7
                                               ,43424, 1.7
                                               ,43436, 1.6
                                               ,43437, 1.6
                                               ,43438, 1.6
                                               ,43439, 1.8
                                               ,43423, 1.8
                                               ,221926, 1.7
                                               ,43427, 1.8
                                               ,43422, 1.4
                                               ,43421, 1.2
                                               ,43440, 1.8
                                               ,43441, 2
                                               ,43433, 2.5
                                               ,43435, 1.5
                                               ,43434, 1.7
                                               ,43432, 1.7
                                               ,43431, 1.5
                                               ,43430, 1.8
                                               ,43429, 1.4
                                               ,45490, 1.8
                                               ,45491, 1.8
                                               ,45364, 1.8
                                               ,45367, 1.5
                                               ,45373, 1.4
                                               ,45372, 1.9
                                               ,45371, 1.5
                                               ,45370, 1
                                               ,45369, 1.8
                                               ,45368, 1.8
                                               ,45375, 1.8
                                               ,45374, 1.7
                                               ,45481, 1
                                               ,45482, 1.2
                                               ,45483, 1.3
                                               ,45484, 1.8
                                               ,45366, 1.6
                                               ,45365, 1.7
                                               ,45485, 1.9
                                               ,45486, 1.8
                                               ,45487, 1.9
                                               ,45488, 1.9
                                               ,45489, 1.8
                                               ,49709, 1.9
                                               ,49710, 1.9
                                               ,49711, 2.1
                                               ,49712, 2.1
                                               ,49713, 2
                                               ,49719, 1.9
                                               ,49718, 2
                                               ,49717, 1.8
                                               ,49716, 1.1
                                               ,49715, 1.8
                                               ,49714, 1.6
                                               ,49702, 2
                                               ,49720, 1.4
                                               ,49721, 1.4
                                               ,49722, 1.6
                                               ,49723, 2
                                               ,49706, 2
                                               ,49703, 1.9
                                               ,49704, 1.8
                                               ,49705, 2
                                               ,49707, 1.5
                                               ,49708, 1.9
                                               ,52327, 2
                                               ,52326, 2.1
                                               ,52325, 2
                                               ,52322, 1.7
                                               ,52314, 2.1
                                               ,52315, 1.9
                                               ,52316, 1.5
                                               ,52328, 1.5
                                               ,52329, 1.6
                                               ,52323, 1.5
                                               ,52309, 1.3
                                               ,52310, 1.3
                                               ,52311, 1.7
                                               ,52313, 2
                                               ,52312, 1.9
                                               ,52320, 1.5
                                               ,52321, 1.6
                                               ,52317, 1.9
                                               ,52324, 1.6
                                               ,52318, 2
                                               ,52319, 1.6
                                               ,52308, 2.1
                                               ,57363, 2
                                               ,63388, 1.8
                                               ,63368, 1.9
                                               ,63370, 2
                                               ,63338, 2
                                               ,68059, 2.3
                                               ,68058, 2
                                               ,68057, 2.2
                                               ,68056, 2
                                               ,68050, 1.2
                                               ,68051, 2
                                               ,68053, 1.7
                                               ,68052, 1.3
                                               ,68054, 1.8
                                               ,68055, 2
                                               ,68049, 1.9
                                               ,68048, 1.5
                                               ,68047, 1.5
                                               ,68046, 1.7
                                               ,68045, 2.1
                                               ,68044, 2
                                               ,68042, 1.9
                                               ,68041, 2.2
                                               ,68043, 2.1
                                               ,68040, 2.4
                                               ,68039, 2
                                               ,68038, 1.7
                                               ,182037, 1.7
                                               ,294691, 1.5
                                               ,195806, 1.5
                                               ,72200, 1.3
                                               ,72199, 2
                                               ,72196, 2.4
                                               ,72185, 2
                                               ,72184, 2
                                               ,72183, 2
                                               ,72179, 2.1
                                               ,72180, 2.3
                                               ,72181, 2.2
                                               ,72177, 2.1
                                               ,72182, 2
                                               ,72187, 2.3
                                               ,72186, 1.8
                                               ,72211, 2.1
                                               ,72198, 1.6
                                               ,72210, 2
                                               ,72208, 2.1
                                               ,72191, 2
                                               ,72165, 2
                                               ,72169, 1.7
                                               ,72167, 1.6
                                               ,72206, 1.8
                                               ,72205, 2.2
                                               ,72203, 2.4
                                               ,72204, 2.3
                                               ,65702, 1.5
                                               ,65543, 1.8
                                               ,65544, 2.1
                                               ,65546, 2
                                               ,65571, 1.8
                                               ,65572, 2
                                               ,65575, 2
                                               ,65698, 2.1
                                               ,65697, 2
                                               ,65692, 2.2
                                               ,65699, 2
                                               ,65700, 2.2
                                               ,65701, 2
                                               ,65573, 2
                                               ,65562, 1.9
                                               ,65563, 1.6
                                               ,65561, 2
                                               ,65542, 2.2
                                               ,65541, 2
                                               ,65540, 1.8
                                               ,65559, 1.6
                                               ,65707, 1.6
                                               ,65708, 1.9
                                               ,65714, 1.8
                                               ,65716, 2.4
                                               ,65705, 2
                                               ,65538, 2.4
                                               ,65704, 2
                                               ,65703, 2
                                               ,65718, 2.3
                                               ,65715, 2
                                               ,65717, 2.4
                                               ,65720, 2.2
                                               ,65721, 2
                                               ,65723, 2.2
                                               ,65725, 2
                                               ,65706, 2
                                               ,65724, 2
                                               ,65722, 2.2
                                               ,65547, 2
                                               ,65548, 2.4
                                               ,65550, 2.2
                                               ,65551, 1.9
                                               ,65552, 1.5
                                               ,65554, 1.5
                                               ,65555, 2
                                               ,65557, 2
                                               ,65558, 2.1
                                               ,65539, 2
                                               ,65713, 2
                                               ,65709, 2.1
                                               ,65712, 2.2
                                               ,65711, 2.2
                                               ,65710, 1.9
                                               ,59909, 1.5
                                               ,59887, 2.2
                                               ,59906, 2
                                               ,59893, 2
                                               ,59888, 2.3
                                               ,59889, 2.4
                                               ,59891, 1.5
                                               ,59892, 2.1
                                               ,59896, 2.1
                                               ,57535, 2
                                               ,57544, 2
                                               ,49359, 1.4
                                               ,49349, 1.7
                                               ,49353, 1.8
                                               ,49355, 2
                                               ,49356, 1.9
                                               ,49366, 1.4
                                               ,49372, 2.2
                                               ,49389, 1.5
                                               ,49390, 1.6
                                               ,45838, 1.6
                                               ,45837, 1.7
                                               ,45836, 2
                                               ,45940, 2
                                               ,45939, 1.9
                                               ,45953, 2.2
                                               ,45952, 2
                                               ,45945, 2
                                               ,45947, 1.6
                                               ,45949, 2.2
                                               ,45950, 1.9
                                               ,45948, 1.6
                                               ,45946, 2.2
                                               ,45944, 1.4
                                               ,45943, 1.9
                                               ,45942, 2
                                               ,45951, 1.7
                                               ,45852, 2
                                               ,45851, 2.4
                                               ,45850, 2.3
                                               ,45853, 2
                                               ,45941, 1.7
                                               ,45854, 1.4
                                               ,45849, 1.9
                                               ,45856, 2.1
                                               ,45857, 1.9
                                               ,45855, 2.2
                                               ,45839, 2.4
                                               ,45846, 2.3
                                               ,45847, 2.4
                                               ,45860, 2.3
                                               ,45858, 2.2
                                               ,45859, 2
                                               ,45861, 2.2
                                               ,45862, 2.1
                                               ,45867, 2.4
                                               ,45866, 2.6
                                               ,45848, 2.3
                                               ,45865, 2.5
                                               ,45864, 2.2
                                               ,45863, 2
                                               ,45935, 1.5
                                               ,45938, 2.6
                                               ,45937, 2
                                               ,45936, 2.1
                                               ,45934, 1.4
                                               ,45933, 2
                                               ,45932, 2
                                               ,45931, 2.2
                                               ,45845, 2
                                               ,45843, 1.6
                                               ,45844, 2.4
                                               ,45842, 1.8
                                               ,45840, 2
                                               ,45841, 2
                                               ,45954, 2.2
                                               ,42624, 1.6
                                               ,42623, 1.4
                                               ,42622, 2
                                               ,42621, 2
                                               ,42620, 2.2
                                               ,42619, 2
                                               ,42607, 2
                                               ,42611, 1.6
                                               ,42613, 2
                                               ,42614, 2.1
                                               ,42612, 2
                                               ,42610, 2.2
                                               ,42608, 2.2
                                               ,42599, 1.9
                                               ,42598, 1.7
                                               ,42604, 2
                                               ,42602, 2
                                               ,42605, 2
                                               ,42606, 2
                                               ,42603, 1.4
                                               ,42595, 1.7
                                               ,42596, 2
                                               ,42625, 1.7
                                               ,42616, 1.9
                                               ,42617, 1.9
                                               ,42597, 2.3
                                               ,42525, 2
                                               ,42526, 2
                                               ,42527, 2.3
                                               ,42626, 2.3
                                               ,42527, 2.3
                                               ,42626, 2.3
                                               ,42618, 1.9
                                               ,42627, 2.1
                                               ,42628, 2
                                               ,42528, 2
                                               ,42530, 2.3
                                               ,42529, 2.4
                                               ,42615, 2.2
                                               ,42518, 2
                                               ,42517, 2.2
                                               ,42516, 2
                                               ,42515, 2.2
                                               ,42514, 2
                                               ,43150, 2.3
                                               ,42513, 1.5
                                               ,42512, 1.7
                                               ,42511, 2
                                               ,42510, 2.3
                                               ,42509, 2
                                               ,42524, 2
                                               ,42519, 2
                                               ,42523, 2
                                               ,42522, 2.2
                                               ,42520, 2
                                               ,42521, 2
                                               ,42609, 1.9
                                               ,40383, 2
                                               ,40390, 2
                                               ,40404, 2.2
                                               ,40405, 1.7
                                               ,40403, 2
                                               ,40408, 1.9
                                               ,40395, 1.6
                                               ,40399, 1.5
                                               ,40375, 1.5
                                               ,40410, 1.5
                                               ,40409, 1.3
                                               ,37556, 1
                                               ,37558, 1.8
                                               ,37559, 2
                                               ,37624, 1.8
                                               ,37625, 1.6
                                               ,37626, 1.9
                                               ,37785, 1.4
                                               ,37786, 1.3
                                               ,37787, 2
                                               ,37788, 1
                                               ,37789, 1.4
                                               ,37790, 2
                                               ,37791, 1.9
                                               ,37792, 1.3
                                               ,37793, 2
                                               ,37794, 1.9
                                               ,37795, 1.6
                                               ,37796, 1.9
                                               ,37797, 2
                                               ,37798, 2.2
                                               ,37799, 1.9
                                               ,37800, 1.7
                                               ,37801, 1.6
                                               ,37802, 2
                                               ,37803, 1.9
                                               ,37804, 1.8
                                               ,37805, 2
                                               ,37806, 2
                                               ,37807, 2
                                               ,37808, 1.9
                                               ,37809, 2
                                               ,37810, 2.3
                                               ,37811, 2
                                               ,37812, 1.4
                                               ,37813, 1.4
                                               ,37814, 1.8
                                               ,37815, 1.5
                                               ,37816, 2
                                               ,37817, 2.1
                                               ,37818, 1.9
                                               ,37819, 2
                                               ,37820, 2.1
                                               ,37821, 2.1
                                               ,37822, 1.7
                                               ,37823, 1.7
                                               ,37824, 2.2
                                               ,37825, 1.5
                                               ,67011, 1.7
                                               ,67015, 1.5
                                               ,67016, 1.7
                                               ,67020, 2
                                               ,67021, 2
                                               ,41939, 1.8
                                               ,41938, 2
                                               ,41937, 1.8
                                               ,41936, 1.8
                                               ,41928, 1.8
                                               ,41929, 2
                                               ,41927, 2
                                               ,41925, 2.1
                                               ,41924, 2
                                               ,41923, 2
                                               ,41922, 1.7
                                               ,41926, 1.9
                                               ,41930, 2.3
                                               ,41921, 1.8
                                               ,41914, 1.7
                                               ,41931, 1.6
                                               ,41932, 1.9
                                               ,41915, 1.9
                                               ,41916, 1.9
                                               ,41917, 1.6
                                               ,41933, 1.2
                                               ,41934, 1.7
                                               ,41940, 2
                                               ,41918, 1.6
                                               ,41919, 1.5
                                               ,41935, 2.1
                                               ,41943, 2
                                               ,41942, 1.6
                                               ,41941, 2
                                               ,41908, 1.9
                                               ,41920, 1.7
                                               ,41909, 2
                                               ,41910, 2
                                               ,41911, 1.7
                                               ,41913, 2
                                               ,41912, 2.1
                                               ,221776, 1.9
                                               ,42007, 2
                                               ,221775, 2
                                               ,42008, 1.4
                                               ,42009, 2
                                               ,42010, 1.9
                                               ,42011, 1.9
                                               ,42012, 1.3
                                               ,42013, 1.3
                                               ,42014, 2
                                               ,42015, 1.9
                                               ,42016, 2
                                               ,41907, 2
                                               ,42017, 1.7
                                               ,41906, 1.9
                                               ,41905, 2
                                               ,42018, 1.9
                                               ,41904, 2
                                               ,45345, 2
                                               ,45344, 1.9
                                               ,45339, 2
                                               ,45340, 1.7
                                               ,45341, 1.9
                                               ,45342, 2
                                               ,45175, 2
                                               ,45179, 2
                                               ,45178, 2.1
                                               ,45177, 2.1
                                               ,45176, 2
                                               ,45180, 1.9
                                               ,45187, 1.9
                                               ,45185, 2.1
                                               ,45186, 1.9
                                               ,45188, 1.7
                                               ,45343, 1.5
                                               ,45184, 1.9
                                               ,45181, 1.5
                                               ,45187, 1.9
                                               ,45185, 2.1
                                               ,45186, 1.9
                                               ,45188, 1.7
                                               ,45343, 1.5
                                               ,45189, 1.4
                                               ,45346, 1.8
                                               ,45191, 1.7
                                               ,45232, 2
                                               ,45192, 1.8
                                               ,45347, 1.5
                                               ,45348, 1.8
                                               ,45349, 1.9
                                               ,45226, 2
                                               ,45231, 1.7
                                               ,45230, 2.1
                                               ,45229, 2.1
                                               ,45228, 2
                                               ,45227, 2.1
                                               ,45327, 2
                                               ,45190, 2
                                               ,45328, 1.8
                                               ,45329, 2
                                               ,45330, 1.4
                                               ,45338, 2
                                               ,45337, 1.9
                                               ,45336, 1.7
                                               ,45335, 1.5
                                               ,45334, 1.3
                                               ,45333, 2
                                               ,45332, 1.9
                                               ,45331, 2.2
                                               ,45174, 2
                                               ,45172, 1.7
                                               ,45173, 2
                                               ,45233, 2
                                               ,45235, 2.2
                                               ,45234, 2
                                               ,48843, 2.2
                                               ,48842, 2
                                               ,48841, 1.9
                                               ,48840, 2.1
                                               ,48839, 2
                                               ,48833, 2
                                               ,49046, 1.9
                                               ,49045, 2
                                               ,49044, 2
                                               ,49042, 1.9
                                               ,49047, 1.8
                                               ,48832, 2.1
                                               ,48831, 2.1
                                               ,48828, 2
                                               ,48829, 1.5
                                               ,48830, 1.4
                                               ,48826, 1.9
                                               ,48825, 2.1
                                               ,48827, 1.7
                                               ,48824, 1.5
                                               ,48837, 2
                                               ,48844, 1.8
                                               ,48838, 1.8
                                               ,49043, 1.9
                                               ,48835, 2
                                               ,48834, 1.9
                                               ,48822, 1.8
                                               ,48821, 1.9
                                               ,48820, 2
                                               ,48823, 1.9
                                               ,48819, 2
                                               ,48817, 1.4
                                               ,48816, 1.9
                                               ,49048, 2
                                               ,48836, 2
                                               ,49049, 1.9
                                               ,49050, 1.8
                                               ,49051, 2
                                               ,48809, 2
                                               ,48808, 1.9
                                               ,49057, 2
                                               ,49056, 1.5
                                               ,49055, 1.3
                                               ,49054, 2
                                               ,49053, 2
                                               ,49052, 2
                                               ,48815, 2
                                               ,48810, 2.1
                                               ,48814, 2.1
                                               ,48813, 1.9
                                               ,48811, 1.7
                                               ,48812, 2.2
                                               ,52247, 1.9
                                               ,52253, 2
                                               ,52255, 1.9
                                               ,52257, 1.9
                                               ,52258, 2.1
                                               ,52259, 2
                                               ,52290, 2
                                               ,52291, 1.5
                                               ,52293, 2.1
                                               ,52294, 2.1
                                               ,52292, 2.1
                                               ,52288, 2
                                               ,52289, 2.2
                                               ,52287, 1.9
                                               ,52286, 1.8
                                               ,52260, 1.7
                                               ,52285, 2.1
                                               ,52281, 1.6
                                               ,52282, 1.8
                                               ,52280, 1.9
                                               ,52284, 2.1
                                               ,52279, 2
                                               ,52283, 1.8
                                               ,52278, 1.8
                                               ,52261, 1.7
                                               ,52276, 2.2
                                               ,52275, 2
                                               ,52274, 1.7
                                               ,52273, 1.9
                                               ,52272, 2.1
                                               ,52262, 1.9
                                               ,52263, 2.1
                                               ,52264, 1.9
                                               ,52300, 1.7
                                               ,52265, 1.8
                                               ,52299, 2
                                               ,52277, 2.2
                                               ,52298, 2
                                               ,52297, 2
                                               ,52296, 2
                                               ,52307, 2.1
                                               ,52306, 2
                                               ,52305, 2
                                               ,52304, 1.5
                                               ,52303, 2
                                               ,52302, 2
                                               ,52301, 1.8
                                               ,52295, 2
                                               ,52266, 2
                                               ,52271, 2.2
                                               ,52267, 1.8
                                               ,52268, 2.2
                                               ,52270, 1.7
                                               ,52269, 2
                                               ,57401, 1.8
                                               ,57414, 1.9
                                               ,63394, 2
                                               ,63419, 2.3
                                               ,63450, 1.7
                                               ,63453, 1.5
                                               ,71319, 2
                                               ,71320, 1.6
                                               ,71294, 2
                                               ,71321, 1.9
                                               ,71322, 1.9
                                               ,71307, 2
                                               ,71323, 2
                                               ,71309, 2
                                               ,71312, 2
                                               ,71311, 2
                                               ,71310, 2.4
                                               ,71313, 1.5
                                               ,71314, 1.5
                                               ,71315, 1.5
                                               ,70062, 2.3
                                               ,70061, 2.2
                                               ,71308, 1.5
                                               ,71306, 2.2
                                               ,70060, 2
                                               ,70059, 2
                                               ,71304, 1.8
                                               ,71305, 1.4
                                               ,71303, 1.6
                                               ,71318, 2
                                               ,70057, 2
                                               ,70058, 1.8
                                               ,70055, 2.2
                                               ,71302, 1.7
                                               ,70050, 2
                                               ,70049, 2.2
                                               ,71295, 2.1
                                               ,71296, 1.8
                                               ,71297, 2
                                               ,71298, 2
                                               ,71299, 2
                                               ,71301, 2.3
                                               ,71300, 2.2
                                               ,70056, 2.3
                                               ,70432, 2
                                               ,70430, 2
                                               ,70063, 2
                                               ,70064, 2.1
                                               ,70065, 2.1
                                               ,70066, 2
                                               ,70067, 1.3
                                               ,70068, 1.6
                                               ,70069, 2
                                               ,70070, 2.3
                                               ,70071, 2.1
                                               ,71317, 2
                                               ,71316, 2
                                               ,70051, 2
                                               ,70054, 2.3
                                               ,70052, 2.1
                                               ,70053, 2
                                               ,70433, 2.2
                                               ,70434, 1.5
                                               ,73842, 2
                                               ,80800, 2.3
                                               ,100619, 1.8
                                               ,100622, 2
                                               ,100658, 1.9
                                               ,182149, 1.1
                                               ,307484, 2
                                               ,307483, 2
                                               ,231379, 3
                                               ,52710, 3.9
                                               ,49304, 3
                                               ,49299, 3
                                               ,49300, 3
                                               ,49301, 2
                                               ,49303, 2
                                               ,49305, 3
                                               ,49306, 3
                                               ,49307, 3
                                               ,49308, 3
                                               ,49058, 3
                                               ,49059, 3
                                               ,49060, 3
                                               ,49061, 3
                                               ,49063, 2
                                               ,49062, 2
                                               ,40437, 2
                                               ,40439, 2
                                               ,161169, 2.2
                                               ,161170, 2.2
                                               ,161189, 4.3
                                               ,161196, 4
                                               ,161208, 4
                                               ,86207, 1.5
                                               ,86379, 1.8
                                               ,42624, 1.6
                                               ,45838, 1.6
                                               ,41939, 1.8
                                               ,45345, 2
                                               ,48843, 2.2
                                               ,42623, 1.5
                                               ,45837, 1.7
                                               ,41938, 2
                                               ,45344, 2
                                               ,45842, 2
                                               ,42622, 2
                                               ,45836, 2
                                               ,45940, 2
                                               ,41937, 1.8
                                               ,45339, 2
                                               ,48841, 2
                                               ,42621, 2
                                               ,45939, 1.9
                                               ,41936, 1.8
                                               ,45340, 1.8
                                               ,48840, 2.1
                                               ,42620, 2.2
                                               ,45953, 2.2
                                               ,41928, 1.8
                                               ,45341, 2
                                               ,48839, 2
                                               ,42619, 2
                                               ,45952, 2
                                               ,41929, 2
                                               ,45342, 2
                                               ,48833, 2
                                               ,42607, 2
                                               ,45945, 2
                                               ,49366, 1.8
                                               ,41927, 2
                                               ,45175, 1.8
                                               ,42611, 1.6
                                               ,45947, 1.6
                                               ,41925, 2.1
                                               ,45179, 2
                                               ,49046, 2
                                               ,42613, 1.9
                                               ,45949, 2.2
                                               ,41924, 2
                                               ,45178, 2.1
                                               ,49045, 2
                                               ,42614, 2.1
                                               ,45950, 1.9
                                               ,41923, 2
                                               ,45177, 2.1
                                               ,49044, 2
                                               ,42612, 2
                                               ,45948, 1.6
                                               ,41922, 1.7
                                               ,45176, 2
                                               ,49042, 1.9
                                               ,42610, 2.2
                                               ,45946, 2.2
                                               ,41926, 1.9
                                               ,45180, 1.9
                                               ,49047, 1.8
                                               ,45944, 1.4
                                               ,49372, 2.1
                                               ,41930, 2.3
                                               ,45182, 2.1
                                               ,48832, 2.1
                                               ,42608, 2.2
                                               ,45943, 1.9
                                               ,41921, 1.8
                                               ,45183, 2.1
                                               ,48831, 2.1
                                               ,42599, 1.9
                                               ,45942, 2
                                               ,41914, 1.7
                                               ,45184, 1.9
                                               ,48828, 2
                                               ,42598, 1.7
                                               ,45951, 1.7
                                               ,41931, 1.6
                                               ,45181, 1.5
                                               ,48829, 1.4
                                               ,48830, 1.4
                                               ,42604, 2
                                               ,45852, 2
                                               ,41932, 1.9
                                               ,45187, 1.9
                                               ,48826, 1.9
                                               ,42602, 2.1
                                               ,42605, 2
                                               ,45851, 2.4
                                               ,41915, 1.9
                                               ,45185, 2
                                               ,41915, 1.9
                                               ,45185, 2.1
                                               ,42606, 2.1
                                               ,45850, 2.3
                                               ,41916, 1.9
                                               ,48825, 2.1
                                               ,42603, 1.4
                                               ,45853, 2
                                               ,41917, 1.6
                                               ,45188, 1.7
                                               ,48827, 1.7
                                               ,42595, 1.7
                                               ,45941, 1.7
                                               ,41933, 1.2
                                               ,45343, 1.5
                                               ,48824, 1.5
                                               ,42596, 2
                                               ,45854, 1.4
                                               ,41934, 1.7
                                               ,45189, 1.4
                                               ,48837, 1.8
                                               ,42625, 1.7
                                               ,45849, 1.9
                                               ,41940, 1.9
                                               ,45346, 1.8
                                               ,48844, 1.8
                                               ,42616, 1.9
                                               ,45856, 2.1
                                               ,41918, 1.6
                                               ,45191, 1.7
                                               ,48838, 1.8
                                               ,42617, 1.8
                                               ,45857, 1.9
                                               ,41919, 1.5
                                               ,45232, 2
                                               ,49043, 1.9
                                               ,42597, 2.3
                                               ,45855, 2.2
                                               ,41935, 2.1
                                               ,45192, 1.8
                                               ,48835, 2
                                               ,42525, 2.1
                                               ,45839, 2.4
                                               ,41943, 2
                                               ,45347, 1.5
                                               ,48834, 1.9
                                               ,42526, 2
                                               ,45846, 2.3
                                               ,41942, 1.6
                                               ,45348, 1.8
                                               ,48822, 1.8
                                               ,42527, 2.3
                                               ,45847, 2.4
                                               ,41941, 2
                                               ,45349, 1.9
                                               ,48821, 1.9
                                               ,42626, 2.3
                                               ,45860, 2.3
                                               ,49353, 1.8
                                               ,41908, 1.9
                                               ,45226, 2
                                               ,48820, 2
                                               ,42618, 1.9
                                               ,45858, 2.2
                                               ,49356, 1.9
                                               ,41920, 1.7
                                               ,45231, 1.7
                                               ,48823, 1.9
                                               ,42627, 2.1
                                               ,45859, 2
                                               ,49355, 1.9
                                               ,41909, 2
                                               ,45230, 2.1
                                               ,48819, 2
                                               ,42628, 2
                                               ,45861, 2.2
                                               ,41910, 2
                                               ,48818, 1.5
                                               ,42528, 2.1
                                               ,45862, 2.1
                                               ,41911, 1.7
                                               ,45228, 2
                                               ,48817, 1.4
                                               ,42530, 2.4
                                               ,45867, 2.4
                                               ,41913, 2
                                               ,45227, 2.1
                                               ,48816, 1.9
                                               ,42529, 2.4
                                               ,45866, 2.6
                                               ,41912, 2.1
                                               ,45327, 2
                                               ,49048, 2
                                               ,42615, 2.2
                                               ,45848, 2.3
                                               ,45190, 2
                                               ,221776, 1.9
                                               ,48836, 2
                                               ,42518, 1.9
                                               ,45865, 2.5
                                               ,42007, 2.1
                                               ,45328, 1.8
                                               ,49049, 1.9
                                               ,42517, 2.2
                                               ,45864, 2.2
                                               ,45329, 2
                                               ,221775, 2.1
                                               ,49050, 1.8
                                               ,42516, 2
                                               ,45863, 2
                                               ,45935, 1.5
                                               ,42008, 2
                                               ,45330, 2
                                               ,49051, 2
                                               ,42515, 2.2
                                               ,45938, 2.6
                                               ,42009, 2.1
                                               ,45338, 2
                                               ,48809, 2
                                               ,42514, 2.1
                                               ,45937, 2
                                               ,42010, 2
                                               ,45337, 1.9
                                               ,48808, 1.9
                                               ,43150, 2.3
                                               ,45936, 2.1
                                               ,42011, 2
                                               ,45336, 1.7
                                               ,49057, 2
                                               ,42513, 1.2
                                               ,49390, 1.6
                                               ,42012, 1.3
                                               ,45335, 1.6
                                               ,49056, 1.5
                                               ,42512, 1.7
                                               ,45934, 1.4
                                               ,49389, 1.4
                                               ,42013, 1.3
                                               ,45334, 1.3
                                               ,49055, 1.3
                                               ,42511, 2
                                               ,45933, 2
                                               ,42014, 2
                                               ,45333, 2
                                               ,49054, 2
                                               ,42510, 2.3
                                               ,45932, 2
                                               ,42015, 1.9
                                               ,45332, 1.9
                                               ,49053, 2
                                               ,42509, 2
                                               ,45931, 2.2
                                               ,42016, 2
                                               ,45331, 2.1
                                               ,49052, 2
                                               ,42524, 2
                                               ,45845, 2
                                               ,41907, 2
                                               ,45174, 2
                                               ,48815, 2
                                               ,42519, 2.1
                                               ,45843, 1.6
                                               ,49349, 1.7
                                               ,42017, 1.7
                                               ,45172, 1.7
                                               ,48810, 2.1
                                               ,42523, 2
                                               ,45844, 2.4
                                               ,41906, 2
                                               ,45173, 2
                                               ,48814, 2
                                               ,42522, 2.2
                                               ,45842, 1.8
                                               ,41905, 2
                                               ,45233, 2
                                               ,48813, 1.9
                                               ,45840, 2
                                               ,42018, 1.9
                                               ,45235, 2.2
                                               ,48811, 1.7
                                               ,42521, 2
                                               ,45841, 2
                                               ,41904, 2
                                               ,45234, 2
                                               ,48812, 2.2
                                               ,42609, 1.9
                                               ,45954, 2.2
                                               ,42520, 1.5
                                               ,46368, 1.5
                                               ,49896, 2.1
                                               ,49895, 2
                                               ,46369, 1.9
                                               ,49894, 2.2
                                               ,46370, 2.1
                                               ,49893, 2.1
                                               ,46371, 2
                                               ,49892, 1.9
                                               ,46372, 1.7
                                               ,49890, 2
                                               ,46373, 2
                                               ,49889, 2
                                               ,46374, 1.5
                                               ,49888, 1.7
                                               ,46375, 2.1
                                               ,49881, 2
                                               ,46376, 2
                                               ,49882, 2
                                               ,46377, 2.6
                                               ,49883, 1.9
                                               ,46378, 1.9
                                               ,49885, 1.8
                                               ,46379, 1.9
                                               ,49884, 1.9
                                               ,46380, 1.9
                                               ,49886, 2
                                               ,46381, 2
                                               ,49887, 1.5
                                               ,46382, 2.1
                                               ,49891, 1.2
                                               ,46383, 1.5
                                               ,49880, 2
                                               ,49879, 1.7
                                               ,49878, 1.6
                                               ,46851, 3
                                               ,49546, 0.7
                                               ,46848, 2.7
                                               ,42468, 2
                                               ,42467, 2
                                               ,42465, 2
                                               ,42466, 2
                                               ,57425, 1.5
                                               ,49348, 1
                                               ,49302, 2
                                               ,45198, 2.5
                                               ,42469, 2
                                               ,42470, 2.2
                                               ,45194, 2.2
                                               ,45193, 2.1
                                               ,358290, 1.8
                                               ,52723, 4
                                               ,45728, 3.3
                                               ,45730, 4
                                               ,45731, 2.7
                                               ,45732, 3
                                               ,45733, 3.7
                                               ,45734, 3
                                               ,45735, 3
                                               ,45736, 2
                                               ,45737, 2
                                               ,52725, 4
                                               ,52726, 4
                                               ,52727, 2
                                               ,52729, 4
                                               ,52731, 4
                                               ,64941, 4
                                               ,64942, 4
                                               ,64943, 4
                                               ,168242, 2.5
                                               ,168246, 2.5
                                               ,168248, 1.8
                                               ,196867, 2
                                               #LHU1-91 - hunafl.vor
                                               ,47232, 1
                                               ,47231, 1.5
                                               ,47230, 2
                                               ,47229, 2
                                               ,47752, 1.5
                                               ,47751, 2
                                               ,47750, 1.8
                                               ,47749, 2
                                               ,47748, 1.8
                                               ,47747, 2
                                               ,47746, 2
                                               ,47228, 2
                                               ,47227, 2
                                               ,47226, 1.7
                                               ,47745, 2
                                               ,47744, 1.4
                                               ,47225, 1.9
                                               ,47224, 2
                                               ,47743, 2
                                               ,47223, 2
                                               ,47222, 2
                                               ,47742, 1.6
                                               ,47221, 2
                                               ,47220, 1.5
                                               ,47741, 1.4
                                               ,47740, 1.5
                                               ,47219, 1
                                               ,47739, 1
                                               ,47738, 1
                                               ,47218, 1.5
                                               ,47737, 1.5
                                               ,47736, 2
                                               ,47735, 2
                                               ,47216, 2
                                               ,47734, 1
                                               #L1-90-vor axarfj
                                               ,46165, 1
                                               ,46166, 0.8
                                               ,46173, 1
                                               ,46172, 0.8
                                               ,46169, 1.3
                                               ,46174, 1
                                               ,46170, 1
                                               ,46171, 1
                                               ,46168, 1
                                               ,46167, 1
                                               ,46164, 1
                                               ,46163, 1
                                               ,46161, 1.3
                                               ,46159, 1
                                               ,46160, 1
                                               ,46162, 1
                                               ,46175, 1.2
                                               ,42107, 2
                                               ,42106, 2
                                               ,43309, 2
                                               ,43308, 2
                                               ,42101, 1.3
                                               ,42100, 2
                                               ,42102, 2
                                               ,42099, 1.7
                                               ,42104, 2
                                               ,42103, 2
                                               ,42098, 1.7
                                               ,42105, 1.8
                                               ,43307, 2
                                               ,43304, 2
                                               ,43306, 2
                                               ,42092, 1.8
                                               ,42097, 1.8
                                               ,43303, 2
                                               ,43305, 2
                                               ,42096, 2
                                               ,43302, 2
                                               ,43301, 2
                                               ,42095, 2
                                               ,43300, 2
                                               ,43299, 2
                                               ,42094, 1.7
                                               ,42093, 1.2
                                               ,43297, 1.5
                                               ,42087, 2
                                               ,42086, 1.5
                                               ,42088, 1.6
                                               ,42089, 1.7
                                               ,43298, 2
                                               ,42090, 2
                                               #L4-90 vor skagafj
                                               ,45539, 2
                                               ,45538, 2
                                               ,46385, 2
                                               ,45543, 1.3
                                               ,46384, 2
                                               ,45545, 2
                                               ,45544, 2
                                               ,45540, 1.5
                                               ,46386, 1.5
                                               ,45541, 2
                                               ,46390, 2
                                               ,46391, 2.3
                                               ,46389, 2
                                               ,46387, 2.3
                                               ,46388, 1.3
                                               ,45542, 1.5
                                               #LL2-91 vor skagafj
                                               ,49151, 1.8
                                               ,49150, 1.8
                                               ,49149, 1.8
                                               ,49905, 1.6
                                               ,49904, 1.2
                                               ,49903, 1.7
                                               ,49902, 2
                                               ,49901, 2
                                               ,49148, 1.7
                                               ,49147, 1.7
                                               ,49146, 2
                                               ,49900, 2
                                               ,49145, 1.8
                                               ,49899, 2
                                               ,49898, 1.5
                                               ,49897, 1
                                               #
                                               ,312160, 1.8), ncol = 2, byrow = T))
corrected_toglengd_init <- data.frame(corrected_toglengd_init, order = 1:dim(corrected_toglengd_init)[1])
names(corrected_toglengd_init) <- c('synis_id', 'toglengd.fx', 'order')

repeats <-
  corrected_toglengd_init %>% 
  distinct(synis_id, toglengd.fx,.keep_all = TRUE) %>% 
  group_by(synis_id) %>% 
  filter(n()>1) %>% 
  arrange(synis_id, order)

corrected_toglengd <-
  corrected_toglengd_init %>% 
  distinct(synis_id, toglengd.fx,.keep_all = TRUE) %>% 
  filter( !(order %in% repeats$order[seq(2,length(repeats$order),2)]) ) %>% 
  select(-c(order))

dbRemoveTable(mar,'corrected_toglengd')

dbWriteTable(mar, 'corrected_toglengd', corrected_toglengd)


##########fjardarreitir#####################
# arnarfjordur:
sk52 <-data.frame(skiki = 52, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk52$fjardarreitur[sk52$tognumer < 3 | sk52$tognumer > 20] <- 1
sk52$fjardarreitur[((sk52$tognumer > 17 & sk52$tognumer < 21) | sk52$tognumer == 3)] <- 2
sk52$fjardarreitur[(sk52$tognumer == 4 | sk52$tognumer == 5 | sk52$tognumer == 16 | sk52$tognumer == 17)] <- 3
sk52$fjardarreitur[sk52$tognumer > 10 & sk52$tognumer < 16] <- 4
sk52$fjardarreitur[sk52$tognumer > 5 & sk52$tognumer < 11] <- 5
# Isafjordur
sk53 <-data.frame(skiki = 53, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk53$fjardarreitur[sk53$tognumer < 23 ] <- 1
sk53$fjardarreitur[(sk53$tognumer > 22 & sk53$tognumer < 33 | sk53$tognumer == 37)] <- 2
sk53$fjardarreitur[(sk53$tognumer > 32 & sk53$tognumer < 37)] <- 3
sk53$fjardarreitur[(sk53$tognumer == 38 | sk53$tognumer == 39)] <- 4
sk53$fjardarreitur[(sk53$tognumer > 39 & sk53$tognumer < 49)] <- 6
sk53$fjardarreitur[(sk53$tognumer > 48 & sk53$tognumer < 55)] <- 8
# Hunafloi
sk55 <-data.frame(skiki = 55, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk55$fjardarreitur[(sk55$tognumer > 4 & sk55$tognumer < 10)] <- 1
sk55$fjardarreitur[sk55$tognumer < 5 ] <- 2
sk55$fjardarreitur[(sk55$tognumer > 9 & sk55$tognumer < 22)] <- 3
sk55$fjardarreitur[(sk55$tognumer > 21 & sk55$tognumer < 30)] <- 4
sk55$fjardarreitur[(sk55$tognumer > 34 & sk55$tognumer < 38)] <- 5
sk55$fjardarreitur[sk55$tognumer == 38 ] <- 6
sk55$fjardarreitur[(sk55$tognumer > 30 & sk55$tognumer < 35)] <- 7
sk55$fjardarreitur[sk55$tognumer == 30 ] <- 8
sk55$fjardarreitur[sk55$tognumer == 51 ] <- 9
sk55$fjardarreitur[sk55$tognumer == 39 ] <- 10
sk55$fjardarreitur[(sk55$tognumer > 39 & sk55$tognumer < 43) ] <- 11
sk55$fjardarreitur[sk55$tognumer == 53 ] <- 12
sk55$fjardarreitur[(sk55$tognumer > 43 & sk55$tognumer < 47) ] <- 13
sk55$fjardarreitur[sk55$tognumer == 54 ] <- 14
sk55$fjardarreitur[(sk55$tognumer > 47 & sk55$tognumer < 51) ] <- 15
sk55$fjardarreitur[sk55$tognumer %in% c(43,47,52)] <- 0 # fjardarreitur 0 signifies outside fjardarreitur
# Skagafjordur
sk62 <-data.frame(skiki = 62, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk62$fjardarreitur[(sk62$tognumer < 9 | sk62$tognumer == 15) ] <- 1
sk62$fjardarreitur[(sk62$tognumer > 8 & sk62$tognumer < 15) ] <- 2
sk62$fjardarreitur[sk62$tognumer == 17 ] <- 3
sk62$fjardarreitur[(sk62$tognumer == 16 | sk62$tognumer == 18) ] <- 4
# Skjalfandi
sk63 <-data.frame(skiki = 63, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk63$fjardarreitur[sk63$tognumer < 5] <- 1
sk63$fjardarreitur[(sk63$tognumer > 4 & sk63$tognumer < 12)] <- 2
sk63$fjardarreitur[(sk63$tognumer == 12)] <- 0
# Axarfjordur
sk56 <-data.frame(skiki = 56, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk56$fjardarreitur[(sk56$tognumer < 6 | sk56$tognumer == 10) ] <- 1
sk56$fjardarreitur[(sk56$tognumer > 5 & sk56$tognumer < 10 | sk56$tognumer == 11 | 
                      sk56$tognumer == 12 | sk56$tognumer == 14) ] <- 2
sk56$fjardarreitur[((sk56$tognumer > 14 & sk56$tognumer < 18)| sk56$tognumer == 13)] <- 3
sk56$fjardarreitur[(sk56$tognumer > 17 &  sk56$tognumer < 23)] <- 4
# Snaefellsnes
# south
sk31 <-data.frame(skiki = 31, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk31$fjardarreitur[(sk31$tognumer == 2 | sk31$tognumer == 4 | sk31$tognumer == 9) ] <- 8
sk31$fjardarreitur[(sk31$tognumer == 3 | sk31$tognumer == 5) ] <- 8
sk31$fjardarreitur[(sk31$tognumer == 1 | sk31$tognumer == 6) ] <- 7
sk31$fjardarreitur[sk31$tognumer == 10] <- 7
sk31$fjardarreitur[(sk31$tognumer == 7 | sk31$tognumer == 8) ] <- 9
# west
sk32 <-data.frame(skiki = 32, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk32$fjardarreitur[(sk32$tognumer > 0 & sk32$tognumer <9) | (sk32$tognumer > 20 & sk32$tognumer < 25) ] <- 6
sk32$fjardarreitur[(sk32$tognumer == 9 | sk32$tognumer == 19 | sk32$tognumer == 20)] <- 5
# north
sk34 <-data.frame(skiki = 34, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk34$fjardarreitur[(sk34$tognumer == 9 | sk34$tognumer == 19 | sk34$tognumer == 20)] <- 5
sk34$fjardarreitur[(sk34$tognumer == 10 | sk34$tognumer == 11 | sk34$tognumer == 12)] <- 4
sk34$fjardarreitur[(sk34$tognumer == 17 | sk34$tognumer == 18)] <- 3
sk34$fjardarreitur[ sk34$tognumer == 16] <- 2
sk34$fjardarreitur[(sk34$tognumer > 12 & sk34$tognumer < 16)] <- 1
#eldey
sk59 <-data.frame(skiki = 59, tognumer = 1:54, fjardarreitur = rep(NA, 54))
sk59$fjardarreitur[(sk59$tognumer < 3 | sk59$tognumer == 7 | sk59$tognumer == 8)] <- 1
sk59$fjardarreitur[(sk59$tognumer > 2 & sk59$tognumer < 7)] <- 2
sk59$fjardarreitur[sk59$tognumer > 8  & sk59$tognumer < 13] <- 3

corrected_fjardarreitur<-data.frame(rbind(sk31[!is.na(sk31$fjardarreitur),],
                                          sk32[!is.na(sk32$fjardarreitur),],
                                          sk34[!is.na(sk34$fjardarreitur),],
                                          sk52[!is.na(sk52$fjardarreitur),],
                                          sk53[!is.na(sk53$fjardarreitur),],
                                          sk55[!is.na(sk55$fjardarreitur),],
                                          sk56[!is.na(sk56$fjardarreitur),],
                                          sk59[!is.na(sk59$fjardarreitur),],
                                          sk62[!is.na(sk62$fjardarreitur),],
                                          sk63[!is.na(sk63$fjardarreitur),]))
names(corrected_fjardarreitur)[3] <- 'fjardarreitur.fx'

dbRemoveTable(mar,'corrected_fjardarreitur')

dbWriteTable(mar, 'corrected_fjardarreitur', corrected_fjardarreitur)

#from fiskar.numer, used in skala_med_toldum2
#
corrected_afli<-data.frame(matrix(c(40384, 5
                                    ,  265904, 2
                                    ,  333517, 27
                                    ,  325568, 20
                                    ,  63160, 346
                                    ,  86317, 835
                                    ,  93395, 36 # tharf ad ath a stodvabladi!
                                    ,  47748, 714
                                    ,  47743, 280
                                    ,  268451, 4
                                    ,  72234, 136 # tharf ad ath a stodvabladi
                                    ,  59835, 2126
                                    ,  43429, 120
                                    ,  43421, 150
                                    ,  49718, 339 # tharf ad ath a stodvabladi
                                    ,  73482, 219
                                    ,  203237, 1230
                                    ,  57494, 1106
                                    ,  57511, 1354
                                    ,  57484, 2325
                                    ,  57521, 1673
                                    ,  57523, 2496
                                    ,  57527, 1721
                                    ,  57547, 1275
                                    ,  40384, 5
                                    ,  41928, 650
                                    ,  52258, 1200
                                    ,  52289, 1991
                                    ,  120005, 81
                                    ,  167321, 32 # ath stodvablad
                                    ,  91588, 7
                                    ,  81522, 181 # ath stodvablad
                                    ,  45728, 50
                                    ,  45730, 128
                                    ,  45732, 160
                                    ,  45733, 63
                                    ,  45734, 42
                                    ,  45735, 38
                                    ,  45736, 43
                                    ,  45737, 4), ncol = 2, byrow = TRUE), tegund = 41)
names(corrected_afli) <- c('synis_id', 'afli.fx', 'tegund')

dbRemoveTable(mar,'corrected_afli')

dbWriteTable(mar, 'corrected_afli', corrected_afli)


# vigt synis
corrected_vigt_synis <-data.frame(matrix(c(312242, 0.868 # reiknad ut fra fjolda i kg
                                           ,197774, 0.51 # reiknad ut fra fjolda i kg
                                           ,40348, 0.31
                                           ,312177, 0.292 #tgarf samt að skoda hvort thetta passar (og naestu thrjar)
                                           ,287701, 0.944
                                           ,287722, 0.855
                                           ,322153, 1.794), ncol = 2, byrow = TRUE), tegund = 41)
names(corrected_vigt_synis) <- c('synis_id', "vigt_synis.fx", "tegund")

dbRemoveTable(mar,'corrected_vigt_synis')

dbWriteTable(mar, 'corrected_vigt_synis', corrected_vigt_synis)


###### fix functions & tables #####


shrimp_station_fixes <- function(stodvar){
  #stodvar is a query  from fiskar.stodvar
  
  expanded_stodvar <-
    dplyr::left_join(stodvar, tbl(mar,'corrected_skiki')) %>%
    dplyr::left_join(tbl(mar, 'corrected_botnhiti')) %>%
    dplyr::left_join(tbl(mar, 'corrected_yfirbordshiti')) %>%
    dplyr::left_join(tbl(mar, 'corrected_breidd')) %>%
    dplyr::left_join(tbl(mar, 'corrected_tognumer')) %>%
    dplyr::left_join(tbl(mar, 'corrected_togtimi')) %>%
    dplyr::left_join(tbl(mar, 'corrected_toglengd')) %>%
    dplyr::left_join(tbl(mar, 'corrected_fjardarreitur')) %>%
    dplyr::mutate( skiki = nvl2(skiki.fx, skiki.fx, skiki),
                   botnhiti = nvl2(botnhiti.fx, botnhiti.fx, botnhiti),
                   yfirbordshiti = nvl2(yfirbordshiti.fx, yfirbordshiti.fx, yfirbordshiti),
                   kastad_n_breidd = nvl2(kastad_n_breidd.fx, kastad_n_breidd.fx, kastad_n_breidd),
                   tognumer = nvl2(tognumer.fx, tognumer.fx, tognumer),
                   synaflokkur = ifelse( !(leidangur %in% leidangur_full) & synaflokkur == 37, 14, synaflokkur), # record all 37s outside the full leidangur set as 14
                   synaflokkur = ifelse(synaflokkur == 37 & ( #record specific cases of 37 within full leidangur set as 14
                     (skiki == 53 & leidangur %in% isa.h & veidarfaeri == 30) |
                       (skiki == 56 & leidangur %in% axa.h & ar <= 1989) |
                       (skiki == 63 & tognumer >= 12) |
                       (skiki == 55 & tognumer >= 47) |
                       (tognumer >= 55 | tognumer == 0)
                   ),
                   14, synaflokkur),
                   togtimi = nvl2(togtimi.fx, togtimi.fx, togtimi),
                   toglengd = nvl2(toglengd.fx, toglengd.fx, toglengd),
                   fjardarreitur = ifelse(!is.na(fjardarreitur.fx) & (leidangur %in% leidangur_full), fjardarreitur.fx, fjardarreitur)
    ) %>%
    select(-c(skiki.fx, botnhiti.fx, yfirbordshiti.fx, kastad_n_breidd.fx, togtimi.fx, toglengd.fx, fjardarreitur.fx))
  
}




skala_med_toldum2<-function (lengdir,biom.teg=list("41" = c(0.000628641104521994, 2.84713109335131,0.1)))
{
  #The end result of this function is 1) counts by length group scaled up to the whole catch size either by numbers in the catch
  #or biomass in the catch, and 2) a mean size-adjusted mean weight that can be summed to form indices 
  ratio <- lesa_numer(lengdir$src) %>% 
    dplyr::left_join(tbl(mar,'corrected_afli')) %>% 
    dplyr::left_join(tbl(mar,'corrected_vigt_synis')) %>% 
    dplyr::mutate( afli = nvl2(afli.fx, afli.fx, afli),
                   vigt_synis = nvl2(vigt_synis.fx, vigt_synis.fx, vigt_synis)) %>% 
    dplyr::select(-c(afli.fx, vigt_synis.fx)) %>% 
    dplyr::mutate(r = ifelse(fj_talid == 0, 1, fj_talid/ifelse(fj_maelt == 0, 1, fj_maelt)),
                  biom.r = ifelse(afli == 0, NA, afli/ifelse(vigt_synis == 0, NA, vigt_synis)),
                  #simplemean_wt that biases toward overestimates of small individuals
                  simplemean_wt = ifelse(vigt_synis == 0, NA, vigt_synis/ifelse(fj_maelt==0, NA, fj_maelt))) %>% 
    dplyr::select(synis_id, tegund, r, biom.r, simplemean_wt, vigt_synis, fj_maelt) 
  
  biom_spp <- as.numeric(names(biom.teg))
  
  biom.mat<-
    tibble::as.tibble(biom.teg) %>% 
    tidyr::gather() %>% 
    dplyr::mutate(ind = rep(c('a','b','s'),length(biom.teg))) %>% 
    tidyr::spread(ind, value) 
  
  if(sum(biom.mat$s>1)){
    'Warning: interval scales > 1 are not recommended'
  }
  
  dbRemoveTable(mar,'biom.mat')
  dbWriteTable(mar, 'biom.mat', biom.mat)
  
  lengdir.tmp <-
    lengdir %>%
    dplyr::left_join(tbl(mar,'biom.mat'), by = c('tegund' = 'key')) %>% 
    dplyr::mutate(lengd_scaler = ifelse(s==0 | is.na(s), 1, 1/s),
                  lengd_interval = round(lengd_scaler*lengd)/lengd_scaler) %>% 
    dplyr::left_join(ratio) 
  
  lengdir.tmp2 <-
    lengdir.tmp %>% 
    dplyr::group_by(synis_id, tegund, lengd_interval) %>%
    dplyr::summarise(fjoldi_by_int = sum(fjoldi)) %>%
    dplyr::right_join(lengdir.tmp)
  
  lengdir.tmp3<- 
    lengdir.tmp2 %>% 
    dplyr::group_by(synis_id, tegund) %>% 
    dplyr::summarise(fjoldi_sum = sum(fjoldi)) %>% 
    dplyr::right_join(lengdir.tmp2) %>% 
    dplyr::mutate(fjoldi_prop_weighted = ifelse(fjoldi_sum*a*lengd_interval^b==0, NA,fjoldi_by_int/fjoldi_sum*a*lengd_interval^b))
  
  lengdir.tmp4<-
    lengdir.tmp3 %>% 
    dplyr::group_by(synis_id,tegund) %>% 
    dplyr::summarise(fjoldi_prop_weighted_sum = sum(fjoldi_prop_weighted)) %>% 
    dplyr::right_join(lengdir.tmp3) %>% 
    dplyr::mutate(biom_prop = ifelse(fjoldi_prop_weighted_sum==0, NA, fjoldi_prop_weighted/fjoldi_prop_weighted_sum)) %>% 
    dplyr::mutate(mean_wt = biom_prop*ifelse(fjoldi_by_int==0, NA, ifelse(vigt_synis == 0, NA, vigt_synis)/fjoldi_by_int)) %>% 
    dplyr::mutate(fjoldi = fjoldi*ifelse(tegund %in% biom_spp, biom.r, r)) %>% 
    dplyr::select(-c(vigt_synis, fj_maelt, simplemean_wt, lengd_interval, lengd_scaler, a, b, s,
                     fjoldi_by_int, fjoldi_sum, fjoldi_prop_weighted, fjoldi_prop_weighted_sum, biom_prop))
  
  return(lengdir.tmp4)
}



#above weight needed to form survey indices
#grunnsl$afli <- ifelse(is.na(grunnsl$afli), 0, grunnsl$afli)
#grunnsl$kg_sjm <- grunnsl$afli / grunnsl$toglengd
#grunnsl$kg_sjm <- ifelse(is.na(grunnsl$kg_sjm), 0, grunnsl$kg_sjm)



