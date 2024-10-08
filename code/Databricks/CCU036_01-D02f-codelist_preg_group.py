# Databricks notebook source
# MAGIC %md ## CCU036_01-D02f-codelist_preg_group
# MAGIC **Description** Describes the codelist used to flag patients with pregnancy codes in the definition of JCVI group.
# MAGIC
# MAGIC **Author** Zach Welshman (adapted from CCU013_03)

# COMMAND ----------

import pandas as pd
import io

# COMMAND ----------

# """ Source primis-covid19-vacc-uptake-preg.csv"""
preg = """
code,term
100801000119107,Maternal tobacco use in pregnancy
10231000132102,In-vitro fertilization pregnancy
102500002,Good neonatal condition at birth
102872000,Pregnancy on oral contraceptive
102875003,Surrogate pregnancy
102876002,Multigravida
102882004,Abnormal placental secretion of chorionic gonadotropin
102885002,Absence of placental secretion of chorionic gonadotropin
102886001,Increased amniotic fluid production
102887005,Decreased amniotic fluid production
102955006,Contraception failure
1031000119109,Insufficient prenatal care
10423003,Braun von Fernwald's sign
10573002,Infection of amniotic cavity
106004004,Hemorrhagic complication of pregnancy
106008001,Delivery AND/OR maternal condition affecting management
106009009,Fetal condition affecting obstetrical care of mother
106010004,Pelvic dystocia AND/OR uterine disorder
106111002,Clinical sign related to pregnancy
10629511000119102,Rhinitis of pregnancy
10741871000119101,Alcohol dependence in pregnancy
10743651000119105,Inflammation of cervix in pregnancy
10743831000119100,Neoplasm of uterus affecting pregnancy
10743881000119104,Suspected fetal abnormality affecting management of mother
10745231000119102,Cardiac arrest due to administration of anesthesia for obstetric procedure in pregnancy
10749871000119100,Malignant neoplastic disease in pregnancy
10750111000119108,Breast lump in pregnancy
10750161000119106,Cholestasis of pregnancy complicating childbirth
10750991000119101,Cyst of ovary in pregnancy
10751771000119107,Placental abruption due to afibrinogenemia
10753491000119101,Gestational diabetes mellitus in childbirth
10755951000119102,Heart murmur in mother in childbirth
10756261000119102,Physical abuse complicating childbirth
10756301000119105,Physical abuse complicating pregnancy
10759231000119102,Salpingo-oophoritis in pregnancy
10760221000119101,Uterine prolapse in pregnancy
10760541000119109,Traumatic injury to vulva during pregnancy
10760581000119104,Pain in round ligament in pregnancy
10760981000119107,Psychological abuse complicating pregnancy
10761021000119102,Sexual abuse complicating childbirth
10761061000119107,Sexual abuse complicating pregnancy
10761391000119102,Tobacco use in mother complicating childbirth
1076861000000103,Ultrasonography of retained products of conception
1079101000000101,Antenatal screening shows higher risk of Down syndrome
1079111000000104,Antenatal screening shows lower risk of Down syndrome
1079121000000105,Antenatal screening shows higher risk of Edwards and Patau syndromes
1079131000000107,Antenatal screening shows lower risk of Edwards and Patau syndromes
10807061000119103,Liver disorder in mother complicating childbirth
10835571000119102,Antepartum hemorrhage due to disseminated intravascular coagulation
10835971000119109,Anti-A sensitization in pregnancy
10836071000119101,Dislocation of symphysis pubis in pregnancy
109891004,Detached products of conception
109893001,Detached trophoblast
109894007,Retained placenta
110081000119109,Bacterial vaginosis in pregnancy
11082009,Abnormal pregnancy
1109951000000101,Pregnancy insufficiently advanced for reliable antenatal screening
1109971000000105,Pregnancy too advanced for reliable antenatal screening
111208003,Melasma gravidarum
111447004,Placental condition affecting management of mother
11337002,Quickening of fetus
11454006,Failed attempted abortion with amniotic fluid embolism
1148801000000108,Monochorionic monoamniotic triplet pregnancy
1148811000000105,Trichorionic triamniotic triplet pregnancy
1148821000000104,Dichorionic triamniotic triplet pregnancy
1148841000000106,Dichorionic diamniotic triplet pregnancy
1149411000000103,Monochorionic diamniotic triplet pregnancy
1149421000000109,Monochorionic triamniotic triplet pregnancy
1167981000000101,Ultrasound scan for chorionicity
11687002,Gestational diabetes mellitus
11718971000119100,Diarrhea in pregnancy
11763009,Placenta increta
118180006,Finding related to amniotic fluid function
118181005,Finding related to amniotic fluid production
118182003,Finding related to amniotic fluid turnover
118185001,Finding related to pregnancy
118189007,Prenatal finding
118216002,Labor finding
11914001,Transverse OR oblique presentation of fetus
119901000119109,Hemorrhage co-occurrent and due to partial placenta previa
12349003,Danforth's sign
123667006,Abnormal mature chorion
125586008,Disorder of placenta
127363001,"Number of pregnancies, currently pregnant"
127364007,Primigravida
127365008,Gravida 2
127366009,Gravida 3
127367000,Gravida 4
127368005,Gravida 5
127369002,Gravida 6
127370001,Gravida 7
127371002,Gravida 8
127372009,Gravida 9
127373004,Gravida 10
127374005,Gravida more than 10
12803000,High maternal weight gain
12867002,Fetal distress affecting management of mother
129597002,Moderate hyperemesis gravidarum
129598007,Severe hyperemesis gravidarum
12983003,Failed attempted abortion with septic shock
130958001,Disorder of placental circulatory function
130959009,Disorder of amniotic fluid turnover
130960004,Disorder of amniotic fluid production
1323351000000104,First trimester bleeding
13404009,Twin-to-twin blood transfer
134435003,Routine antenatal care
134781000119106,High risk pregnancy due to recurrent miscarriage
135881001,Pregnancy review
13763000,"Gestation period, 34 weeks"
13798002,"Gestation period, 38 weeks"
13866000,Fetal acidemia affecting management of mother
13943000,Failed attempted abortion complicated by embolism
14049007,Chaussier's sign
14094001,Excessive vomiting in pregnancy
14418008,Precocious pregnancy
15196006,Intraplacental hematoma
15230009,Liver disorder in pregnancy
1538006,Central nervous system malformation in fetus affecting obstetrical care
15592002,Previous pregnancy 1
15633004,"Gestation period, 16 weeks"
15643101000119103,Gastroesophageal reflux disease in pregnancy
15663008,Placenta previa centralis
1592005,Failed attempted abortion with uremia
16271000119108,Pyelonephritis in pregnancy
16320551000119109,Bleeding from female genital tract co-occurrent with pregnancy
163403004,On examination - vaginal examination - gravid uterus
163498004,On examination - gravid uterus size
163499007,On examination - fundus 12-16 week size
163500003,On examination - fundus 16-20 week size
163501004,On examination - fundus 20-24 week size
163502006,On examination - fundus 24-28 week size
163504007,On examination - fundus 28-32 week size
163505008,On examination - fundus 32-34 week size
163506009,On examination - fundus 34-36 week size
163507000,On examination - fundus 36-38 week size
163508005,On examination - fundus 38 weeks-term size
163509002,On examination - fundus = term size
163510007,On examination - fundal size = dates
163515002,On examination - oblique lie
163516001,On examination - transverse lie
16356006,Multiple pregnancy
1639007,Abnormality of organs AND/OR soft tissues of pelvis affecting pregnancy
16437531000119105,Ultrasonography for qualitative deepest pocket amniotic fluid volume
164817009,Placental localization
16836391000119105,Pregnancy of left fallopian tube
16836431000119100,Ruptured tubal pregnancy of right fallopian tube
16836571000119103,Ruptured tubal pregnancy of left fallopian tube
16836891000119104,Pregnancy of right fallopian tube
169224002,Ultrasound scan for fetal cephalometry
169225001,Ultrasound scan for fetal maturity
169228004,Ultrasound scan for fetal presentation
169229007,Dating/booking ultrasound scan
169230002,Ultrasound scan for fetal viability
169470007,Combined oral contraceptive pill failure
169471006,Progestogen-only pill failure
169488004,Contraceptive intrauterine device failure - pregnant
169501005,"Pregnant, diaphragm failure"
169508004,"Pregnant, sheath failure"
169524003,Depot contraceptive failure
169533001,Contraceptive sponge failure
169539002,Symptothermal contraception failure
169544009,Postcoital oral contraceptive pill failure
169548007,Vasectomy failure
169550004,Female sterilization failure
169560008,Pregnant - urine test confirms
169561007,Pregnant - blood test confirms
169562000,Pregnant - vaginal examination confirms
169563005,Pregnant - on history
169564004,Pregnant - on abdominal palpation
169565003,Pregnant - planned
169566002,Pregnancy unplanned but wanted
169567006,Pregnancy unplanned and unwanted
169568001,Unplanned pregnancy unknown if child is wanted
169569009,Questionable if pregnancy was planned
169572002,Antenatal care categorized by gravida number
169573007,Antenatal care of primigravida
169574001,Antenatal care of second pregnancy
169575000,Antenatal care of third pregnancy
169576004,Antenatal care of multipara
169578003,Antenatal care: obstetric risk
169579006,Antenatal care: uncertain dates
169582001,Antenatal care: history of stillbirth
169583006,Antenatal care: history of perinatal death
169584000,Antenatal care: poor obstetric history
169585004,Antenatal care: history of trophoblastic disease
169587007,Antenatal care: precious pregnancy
169588002,Antenatal care: elderly primiparous
169589005,Antenatal care: history of infertility
169591002,Antenatal care: social risk
169595006,Antenatal care: history of child abuse
169597003,Antenatal care: medical risk
169598008,Antenatal care: gynecological risk
169600002,Antenatal care: under 5ft tall
169602005,Antenatal care: 10 years plus since last pregnancy
169603000,"Antenatal care: primiparous, under 17 years"
169604006,"Antenatal care: primiparous, older than 30 years"
169605007,"Antenatal care: multiparous, older than 35 years"
169613008,Antenatal care provider
169614002,Antenatal care from general practitioner
169615001,Antenatal care from consultant
169616000,Antenatal - shared care
169620001,Delivery: no place booked
169628008,Delivery booking - length of stay
169646002,Antenatal amniocentesis
169650009,Antenatal amniocentesis wanted
169651008,Antenatal amniocentesis - awaited
169652001,Antenatal amniocentesis - normal
169653006,Antenatal amniocentesis - abnormal
169657007,Antenatal ultrasound scan status
169661001,Antenatal ultrasound scan wanted
169662008,Antenatal ultrasound scan awaited
169667002,Antenatal ultrasound scan for slow growth
169668007,Antenatal ultrasound scan 4-8 weeks
169669004,Antenatal ultrasound scan at 9-16 weeks
169670003,Antenatal ultrasound scan at 17-22 weeks
169711001,Antenatal booking examination
169712008,Antenatal 12 weeks examination
169713003,Antenatal 16 week examination
169714009,Antenatal 20 week examination
169715005,Antenatal 24 week examination
169716006,Antenatal 28 week examination
169717002,Antenatal 30 week examination
169718007,Antenatal 32 week examination
169719004,Antenatal 34 week examination
169720005,Antenatal 35 week examination
169721009,Antenatal 36 week examination
169722002,Antenatal 37 week examination
169723007,Antenatal 38 week examination
169724001,Antenatal 39 week examination
169725000,Antenatal 40 week examination
169726004,Antenatal 41 week examination
169727008,Antenatal 42 week examination
17285009,Intraperitoneal pregnancy
173300003,Disorder of pregnancy
17380002,Failed attempted abortion with acute renal failure
17382005,Cervical incompetence
17433009,Ruptured ectopic pregnancy
17532001,Breech malpresentation successfully converted to cephalic presentation
17594002,Fetal bradycardia affecting management of mother
17787002,Peripheral neuritis in pregnancy
18114009,Prenatal examination and care of mother
19099008,Failed attempted abortion with laceration of bladder
19117007,Placenta reflexa
19228003,Failed attempted abortion with perforation of bladder
19236007,Immature abnormal placenta
19258009,Placenta extrachorales
19363005,Failed attempted abortion with blood-clot embolism
19569008,Mild hyperemesis gravidarum
198620003,Ruptured tubal pregnancy
198624007,Membranous pregnancy
198626009,Mesenteric pregnancy
198627000,Angular pregnancy
19866007,Previous operation to cervix affecting pregnancy
198806007,Failed attempted abortion with genital tract or pelvic infection
198807003,Failed attempted abortion with delayed or excessive hemorrhage
198808008,Failed attempted abortion with damage to pelvic organs or tissues
198809000,Failed attempted abortion with renal failure
198810005,Failed attempted abortion with metabolic disorder
198811009,Failed attempted abortion with shock
198812002,Failed attempted abortion with embolism
198874006,"Failed medical abortion, complicated by genital tract and pelvic infection"
198875007,"Failed medical abortion, complicated by delayed or excessive hemorrhage"
198876008,"Failed medical abortion, complicated by embolism"
198878009,"Failed medical abortion, without complication"
198900002,Placenta previa without hemorrhage - not delivered
198903000,Placenta previa with hemorrhage
198906008,Placenta previa with hemorrhage - not delivered
198910006,Placental abruption - delivered
198911005,Placental abruption - not delivered
198912003,Premature separation of placenta with coagulation defect
198918004,Antepartum hemorrhage with coagulation defect - not delivered
198920001,Antepartum hemorrhage with trauma
198923004,Antepartum hemorrhage with trauma - not delivered
198925006,Antepartum hemorrhage with uterine leiomyoma
198928008,Antepartum hemorrhage with uterine leiomyoma - not delivered
198992004,Eclampsia in pregnancy
199023008,Mild hyperemesis-not delivered
199025001,Hyperemesis gravidarum with metabolic disturbance
199028004,Hyperemesis gravidarum with metabolic disturbance - not delivered
199064003,Post-term pregnancy - not delivered
199088001,Habitual aborter - not delivered
199095005,Peripheral neuritis in pregnancy - not delivered
199101006,Asymptomatic bacteriuria in pregnancy - not delivered
199110003,Infections of kidney in pregnancy
199112006,Infections of the genital tract in pregnancy
199118005,Liver disorder in pregnancy - not delivered
199123005,Fatigue during pregnancy - not delivered
199129009,Herpes gestationis - not delivered
199139003,Pregnancy-induced edema and proteinuria without hypertension
199141002,Gestational edema with proteinuria
199194006,Maternal rubella during pregnancy - baby not yet delivered
199227004,Diabetes mellitus during pregnancy - baby not yet delivered
199246003,Anemia during pregnancy - baby not yet delivered
199254001,Drug dependence during pregnancy - baby not yet delivered
199305006,Complications specific to multiple gestation
199306007,Continuing pregnancy after abortion of one fetus or more
199307003,Continuing pregnancy after intrauterine death of one or more fetuses
199314001,Normal delivery but ante- or post- natal conditions present
199318003,Twin pregnancy with antenatal problem
199322008,Triplet pregnancy with antenatal problem
199326006,Quadruplet pregnancy with antenatal problem
199345002,Unstable lie with antenatal problem
199359009,Oblique lie with antenatal problem
199363002,Transverse lie with antenatal problem
199378009,Multiple pregnancy with malpresentation
199381004,Multiple pregnancy with malpresentation with antenatal problem
199397009,Cephalopelvic disproportion
199406006,Generally contracted pelvis with antenatal problem
199410009,Inlet pelvic contraction with antenatal problem
199414000,Outlet pelvic contraction with antenatal problem
199416003,Mixed feto-pelvic disproportion
199419005,Mixed feto-pelvic disproportion with antenatal problem
199423002,Large fetus causing disproportion with antenatal problem
199425009,Hydrocephalic disproportion
199428006,Hydrocephalic disproportion with antenatal problem
199466009,Retroverted incarcerated gravid uterus
199470001,Retroverted incarcerated gravid uterus with antenatal problem
199484006,Cervical incompetence with antenatal problem
199578005,Fetal-maternal hemorrhage with antenatal problem
199583002,Rhesus isoimmunization with antenatal problem
199625002,Placental transfusion syndromes
199647002,Polyhydramnios with antenatal problem
199654008,Oligohydramnios with antenatal problem
199678003,Amniotic cavity infection with antenatal problem
199719009,Elderly primigravida with antenatal problem
199732004,Abnormal findings on antenatal screening of mother
199733009,Abnormal hematologic finding on antenatal screening of mother
199734003,Abnormal biochemical finding on antenatal screening of mother
199735002,Abnormal cytological finding on antenatal screening of mother
199737005,Abnormal radiological finding on antenatal screening of mother
199738000,Abnormal chromosomal and genetic finding on antenatal screening of mother
199741009,Malnutrition in pregnancy
199895009,Vasa previa - delivered
199896005,Vasa previa with antenatal problem
201134009,Alopecia of pregnancy
20259008,Finding of ballottement of fetal parts
20286008,Gorissenne's sign
20391007,Amniotic cyst
206365006,Clostridial intra-amniotic fetal infection
20753005,Hypertensive heart disease complicating AND/OR reason for care during pregnancy
20845005,Meconium in amniotic fluid affecting management of mother
21334005,Failed attempted abortion with oliguria
21346009,Double uterus affecting pregnancy
21623001,Fetal biophysical profile
22173004,Excessive fetal growth affecting management of mother
22281000119101,Post-term pregnancy of 40 to 42 weeks
22288000,Tumor of cervix affecting pregnancy
223003,Tumor of body of uterus affecting pregnancy
2239004,Previous pregnancies 6
22753004,Fetal AND/OR placental disorder affecting management of mother
22758008,Spalding-Horner sign
228471000000102,Routine obstetric scan
228531000000103,Cervical length scanning at 24 weeks
228551000000105,Mid trimester scan
228691000000101,Non routine obstetric scan for fetal observations
228701000000101,Fetal ascites scan
228711000000104,Rhesus detailed scan
22879003,Previous pregnancies 2
228881000000102,Ultrasound monitoring of early pregnancy
228921000000108,Obstetric ultrasound monitoring
22966008,Hypertensive heart AND renal disease complicating AND/OR reason for care during pregnancy
23001005,Stenosis AND/OR stricture of cervix affecting pregnancy
23177005,Crepitus uteri
232671000000100,Fetal measurement scan
234058009,Genital varices in pregnancy
23464008,"Gestation period, 20 weeks"
235888006,Cholestasis of pregnancy
23717007,Benign essential hypertension complicating AND/OR reason for care during pregnancy
237201006,Rectocele complicating antenatal care - baby not yet delivered
237205002,Rectocele affecting obstetric care
237230004,Uremia in pregnancy without hypertension
237233002,Concealed pregnancy
237234008,Undiagnosed pregnancy
237235009,Undiagnosed breech
237236005,Undiagnosed multiple pregnancy
237237001,Undiagnosed twin
237238006,Pregnancy with uncertain dates
237239003,Low risk pregnancy
237240001,Teenage pregnancy
237241002,Viable pregnancy
237243004,Biochemical pregnancy
237244005,Single pregnancy
237247003,Continuing pregnancy after abortion of sibling fetus
237253003,Viable fetus in abdominal pregnancy
237254009,Unruptured tubal pregnancy
237256006,Disorder of pelvic size and disproportion
237257002,Midpelvic contraction
237259004,Variation of placental position
237268002,Chorioangioma
237270006,Antepartum hemorrhage with hypofibrinogenemia
237271005,Antepartum hemorrhage with hyperfibrinolysis
237272003,Antepartum hemorrhage with afibrinogenemia
237273008,Revealed accidental hemorrhage
237274002,Concealed accidental hemorrhage
237275001,Mixed accidental hemorrhage
237276000,Indeterminate antepartum hemorrhage
237277009,Marginal placental hemorrhage
237284001,Symptomatic disorders in pregnancy
237285000,Gestational edema
237286004,Vulval varices in pregnancy
237288003,Abnormal weight gain in pregnancy
237292005,Placental insufficiency
237296008,Morbidly adherent placenta
237297004,Placenta adherent to previous uterine scar
237298009,Maternofetal transfusion
237300009,Pregnancy with isoimmunization
237302001,Kell isoimmunization in pregnancy
237303006,Duffy isoimmunization in pregnancy
237304000,Lewis isoimmunization in pregnancy
238613007,Generalized pustular psoriasis of pregnancy
238820002,Erythema multiforme of pregnancy
239101008,Pregnancy eruption
239102001,Pruritus of pregnancy
239103006,Prurigo of pregnancy
239104000,Pruritic folliculitis of pregnancy
239105004,Transplacental herpes gestationis
240160002,Transient osteoporosis of hip in pregnancy
24095001,Placenta previa partialis
241491007,Ultrasound scan of fetus
241493005,Ultrasound scan for fetal growth
241494004,Ultrasound scan for amniotic fluid volume
2437000,Placenta circumvallata
243826008,Antenatal care status
243827004,Antenatal RhD antibody status
24444009,Failed attempted abortion with sepsis
248985009,Presentation of pregnancy
248996001,Transversely contracted pelvis
249013004,Pregnant abdomen finding
249014005,Finding of shape of pregnant abdomen
249017003,Pregnant uterus displaced laterally
249020006,Cervical observation during pregnancy and labor
249032005,Finding of speed of delivery
249037004,Caul membrane over baby's head at delivery
249064003,"Oblique lie, head in iliac fossa"
249065002,"Oblique lie, breech in iliac fossa"
249089009,Fetal mouth presenting
249090000,Fetal ear presenting
249091001,Fetal nose presenting
249098007,Knee presentation
249099004,Single knee presentation
249100007,Double knee presentation
249104003,Dorsoanterior shoulder presentation
249105002,Dorsoposterior shoulder presentation
249122000,Baby overdue
249207000,Membrane at cervical os
25026004,"Gestation period, 18 weeks"
25032009,Failed attempted abortion with laceration of cervix
25113000,Chorea gravidarum
25585008,Placenta percreta
25749005,Disproportion between fetus and pelvis
25825004,Hemorrhage in early pregnancy
26224003,Failed attempted abortion complicated by genital-pelvic infection
26358008,Ectopic placenta
26623000,Failed attempted termination of pregnancy complicated by delayed and/or excessive hemorrhage
26690008,"Gestation period, 8 weeks"
267197003,"Antepartum hemorrhage, abruptio placentae and placenta previa"
267199000,Antepartum hemorrhage with coagulation defect
267272006,Postpartum coagulation defects
267335003,Fetoplacental problems
267340006,Maternal pyrexia in labor
26828006,Rectocele affecting pregnancy
268445003,Ultrasound scan - obstetric
268585006,Placental infarct
270498000,Malposition and malpresentation of fetus
27068000,Failed attempted abortion with afibrinogenemia
271442007,Fetal anatomy study
27152008,Hyperemesis gravidarum before end of 22 week gestation with carbohydrate depletion
271954000,Failed attempted medical abortion
27388005,Partial placenta previa with intrapartum hemorrhage
273982004,Obstetric problem affecting fetus
274117006,Pregnancy and infectious disease
274118001,Venereal disease in pregnancy
274119009,Rubella in pregnancy
274121004,Cardiac disease in pregnancy
274122006,Gravid uterus - retroverted
275412000,Cystitis of pregnancy
275426009,Pelvic disproportion
276367008,Wanted pregnancy
276445008,Antenatal risk factors
276641008,Intrauterine asphyxia
276642001,Antepartum fetal asphyxia
276881003,Secondary abdominal pregnancy
278056007,Uneffaced cervix
278058008,Partially effaced cervix
280732008,Obstetric disorder of uterus
281307002,Uncertain viability of pregnancy
284075002,Spotting per vagina in pregnancy
28608002,Choriovitelline placenta
28701003,Low maternal weight gain
28911003,Adherent placenta
289203002,Finding of pattern of pregnancy
289204008,Finding of quantity of pregnancy
289205009,Finding of measures of pregnancy
289208006,Finding of viability of pregnancy
289209003,Pregnancy problem
289216002,Finding of second stage of labor
289232004,Finding of third stage of labor
289256000,Mother delivered
289257009,Mother not delivered
289258004,Finding of pattern of delivery
289261003,Delivery problem
289349009,Presenting part ballottable
289350009,Ballottement of fetal head abdominally
289351008,Ballottement of fetal head at fundus
289352001,Ballottement of fetal head in suprapubic area
289353006,Ballottement of fetal head vaginally
289354000,Presenting part not ballottable
289355004,Oblique lie head in right iliac fossa
289356003,Oblique lie head in left iliac fossa
289357007,Oblique lie breech in right iliac fossa
289358002,Oblique lie breech in left iliac fossa
289572007,No liquor observed vaginally
289573002,Finding of passing of operculum
289575009,Operculum not passed
289606001,Maternity pad damp with liquor
289607005,Maternity pad wet with liquor
289608000,Maternity pad soaked with liquor
289675001,Finding of gravid uterus
289676000,Gravid uterus present
289679007,Finding of size of gravid uterus
289680005,Gravid uterus large-for-dates
289681009,Gravid uterus small-for-dates
289682002,Finding of height of gravid uterus
289683007,Fundal height high for dates
289684001,Fundal height equal to dates
289685000,Fundal height low for dates
289686004,Ovoid pregnant abdomen
289687008,Rounded pregnant abdomen
289688003,Transversely enlarged pregnant abdomen
289689006,Finding of arrangement of gravid uterus
289690002,Gravid uterus central
289691003,Gravid uterus deviated to left
289692005,Gravid uterus deviated to right
289693000,Normal position of gravid uterus
289694006,Finding of sensation of gravid uterus
289743008,Finding of upper segment retraction
289744002,Poor retraction of upper segment
289745001,Excessive retraction of upper segment
289746000,Normal retraction of upper segment
289747009,Finding of measures of gravid uterus
289748004,Gravid uterus normal
289749007,Gravid uterus problem
289765008,Ripe cervix
289766009,Finding of thickness of cervix
289768005,Cervix thick
289769002,Cervix thinning
289770001,Cervix thin
289771002,Cervix paper thin
289827009,Finding of cervical cerclage suture
289828004,Cervical cerclage suture absent
29399001,Elderly primigravida
29421008,Failed attempted abortion complicated by renal failure
29851005,Halo sign
301801008,Finding of position of pregnancy
302078000,Abnormal immature chorionic villi
302080006,Finding of birth outcome
302644007,Abnormal immature chorion
30653008,Previous pregnancies 5
307534009,Urinary tract infection in pregnancy
307813007,Antenatal ultrasound scan at 22-40 weeks
308140006,Hemorrhoids in pregnancy
30850008,"Hemorrhage in early pregnancy, antepartum"
310248000,Antenatal care midwifery led
310592002,Pregnancy prolonged - 41 weeks
310594001,Pregnancy prolonged - 42 weeks
31159001,Bluish discoloration of cervix
313017000,Anhydramnios
313178001,Gestation less than 24 weeks
313179009,"Gestation period, 24 weeks"
313180007,Gestation greater than 24 weeks
31383003,Tumor of vagina affecting pregnancy
314204000,Early stage of pregnancy
31563000,Asymptomatic bacteriuria in pregnancy
31601007,Combined pregnancy
31805001,Fetal disproportion
31821006,Uterine scar from previous surgery affecting pregnancy
31998007,"Echography, scan B-mode for fetal growth rate"
33340004,Multiple conception
33348006,Previous pregnancies 7
33370009,Hyperemesis gravidarum before end of 22 week gestation with electrolyte imbalance
33490001,Failed attempted abortion with fat embolism
33552005,Anomaly of placenta
34367002,Failed attempted abortion with perforation of bowel
34478009,Failed attempted abortion with defibrination syndrome
34530006,Failed attempted abortion with electrolyte imbalance
34842007,Antepartum hemorrhage
35381000119101,Quadruplet pregnancy with loss of one or more fetuses
35537000,Ladin's sign
35656003,Intraligamentous pregnancy
35746009,Uterine fibroids affecting pregnancy
36144008,McClintock's sign
36297009,Septate vagina affecting pregnancy
36428009,"Gestation period, 42 weeks"
366323009,Finding of length of gestation
36801000119105,Continuing triplet pregnancy after spontaneous abortion of one or more fetuses
36813001,Placenta previa
36854009,Inlet contraction of pelvis
37005007,"Gestation period, 5 weeks"
370352001,Antenatal screening finding
371106008,Idiopathic maternal thrombocytopenia
371374003,Retained products of conception
371380006,Amniotic fluid leaking
372043009,Hydrops of allantois
373663005,Perinatal period
37762002,Face OR brow presentation of fetus
38010008,Intrapartum hemorrhage
38039008,"Gestation period, 10 weeks"
38099005,Failed attempted abortion with endometritis
386235000,Childbirth preparation
386322007,High risk pregnancy care
38720006,Septuplet pregnancy
3885002,ABO isoimmunization affecting pregnancy
38951007,Failed attempted abortion with laceration of bowel
39101000119109,Pregnancy with isoimmunization from irregular blood group incompatibility
391131000119106,Ultrasonography for qualitative amniotic fluid volume
39120007,Failed attempted abortion with salpingo-oophoritis
39121000119100,Pelvic mass in pregnancy
39208009,Fetal hydrops causing disproportion
39249009,Placentitis
394211000119109,Ultrasonography for fetal biophysical profile with non-stress testing
394221000119102,Ultrasonography for fetal biophysical profile without non-stress testing
3944006,Placental sulfatase deficiency (X-linked steryl-sulfatase deficiency) in a female
397949005,Poor fetal growth affecting management
39804004,Abnormal products of conception
4006006,Fetal tachycardia affecting management of mother
402836009,Spider telangiectasis in association with pregnancy
403528000,Pregnancy-related exacerbation of dermatosis
405736009,Accidental antepartum hemorrhage
40801000119106,Gestational diabetes mellitus complicating pregnancy
408783007,Antenatal Anti-D prophylaxis status
408814002,Ultrasound scan for fetal anomaly
408815001,Ultrasound scan for fetal nuchal translucency
408823004,Antenatal hepatitis B blood screening test status
408825006,Antenatal hepatitis B blood screening test sent
408827003,Antenatal human immunodeficiency virus blood screening test status
408828008,Antenatal human immunodeficiency virus blood screening test sent
408831009,Antenatal thalassemia blood screening test status
408833007,Antenatal thalassemia blood screening test sent
408842000,Antenatal human immunodeficiency virus blood screening test requested
408843005,Antenatal thalassemia blood screening test requested
41215002,"Congenital abnormality of uterus, affecting pregnancy"
41438001,"Gestation period, 21 weeks"
414880004,Nuchal ultrasound scan
415105001,Placental abruption
41587001,Third trimester pregnancy
416413003,Advanced maternal age gravida
417006004,Twin reversal arterial perfusion syndrome
4174008,Placenta tripartita
418090003,Ultrasound obstetric doppler
42102002,"Pre-admission observation, undelivered mother"
42170009,Abnormal amniotic fluid
422808006,Prenatal continuous visit
423445003,Difficulty following prenatal diet
423834007,Difficulty with prenatal rest pattern
424037008,Difficulty following prenatal exercise routine
424441002,Prenatal initial visit
424525001,Antenatal care
424619006,Prenatal visit
42553009,Deficiency of placental barrier function
425551008,Antenatal ultrasound scan for possible abnormality
425708006,Placental aromatase deficiency
426295007,Obstetric uterine artery Doppler
426403007,Late entry into prenatal care
426840007,Fetal biometry using ultrasound
42686001,Chromosomal abnormality in fetus affecting obstetrical care
426997005,Traumatic injury during pregnancy
427013000,Alcohol consumption during pregnancy
427139004,Third trimester bleeding
427623005,Obstetric umbilical artery Doppler
428017002,Condyloma acuminata of vulva in pregnancy
428058009,Gestation less than 9 weeks
428164004,Mitral valve disorder in pregnancy
428230005,Trichomonal vaginitis in pregnancy
428252001,Vaginitis in pregnancy
428511009,Multiple pregnancy with one fetal loss
428566005,Gestation less than 20 weeks
428567001,Gestation 14 - 20 weeks
428930004,Gestation 9- 13 weeks
429187001,Continuing pregnancy after intrauterine death of twin fetus
429240000,Third trimester pregnancy less than 36 weeks
429715006,Gestation greater than 20 weeks
430063002,Transvaginal nuchal ultrasonography
430064008,Transvaginal obstetric ultrasonography
430881000,Second trimester bleeding
430933008,Gravid uterus size for dates discrepancy
43195002,Tenney changes of placenta
432246004,Transvaginal obstetric doppler ultrasonography
43293004,Failed attempted abortion with septic embolism
433153009,Chorionic villus sampling using obstetric ultrasound guidance
43651009,Acquired stenosis of vagina affecting pregnancy
43673002,Fetal souffle
43697006,"Gestation period, 37 weeks"
439311009,Intends to continue pregnancy
43970002,Congenital stenosis of vagina affecting pregnancy
43990006,Sextuplet pregnancy
441697004,Thrombophilia associated with pregnancy
441924001,Gestational age unknown
442478007,Multiple pregnancy involving intrauterine pregnancy and tubal pregnancy
443006,Cystocele affecting pregnancy
443460007,Multigravida of advanced maternal age
44398003,"Gestation period, 4 weeks"
444661007,High risk pregnancy due to history of preterm labor
445866007,Ultrasonography of multiple pregnancy for fetal anomaly
446208007,Ultrasonography in second trimester
446353007,Ultrasonography in third trimester
44640004,Failed attempted abortion with parametritis
446522006,Ultrasonography in first trimester
446810002,Ultrasonography of multiple pregnancy for fetal nuchal translucency
446920006,Transvaginal ultrasonography to determine the estimated date of confinement
44772007,Maternal obesity syndrome
44795003,Rhesus isoimmunization affecting pregnancy
44992005,Failed attempted abortion with intravascular hemolysis
45139008,"Gestation period, 29 weeks"
45307008,Extrachorial pregnancy
45759004,Rigid perineum affecting pregnancy
459166009,Dichorionic diamniotic twin pregnancy
459167000,Monochorionic twin pregnancy
459168005,Monochorionic diamniotic twin pregnancy
459169002,Monochorionic diamniotic twin pregnancy with similar amniotic fluid volumes
459170001,Monochorionic diamniotic twin pregnancy with dissimilar amniotic fluid volumes
459171002,Monochorionic monoamniotic twin pregnancy
46022004,Bearing down reflex
46230007,"Gestation period, 40 weeks"
46365005,Failed attempted abortion with perforation of periurethral tissue
46894009,"Gestational diabetes mellitus, class A>2<"
46906003,"Gestation period, 27 weeks"
47161002,Failed attempted abortion with perforation of cervix
47200007,High risk pregnancy
472321009,Continuing pregnancy after intrauterine death of one twin with intrauterine retention of dead twin
480571000119102,Doppler ultrasound velocimetry of umbilical artery of fetus
48688005,"Gestation period, 26 weeks"
48782003,Delivery normal
4910006,Premature abnormal placenta
49342001,Stricture of vagina affecting pregnancy
49416000,Failed attempted abortion
49964003,Ectopic fetus
50367001,"Gestation period, 11 weeks"
50557007,Healed pelvic floor repair affecting pregnancy
50726009,Failed attempted abortion with perforation of uterus
50844007,Failed attempted abortion with pelvic peritonitis
51195001,Placental polyp
51519001,Marginal insertion of umbilical cord
51885006,Morning sickness
521000119104,"Acute cystitis in pregnancy, antepartum"
5231000179108,Three dimensional obstetric ultrasonography
52327008,Fetal myelomeningocele causing disproportion
5241000179100,Three dimensional obstetric ultrasonography in third trimester
52588004,Robert's sign
53024001,Insufficient weight gain of pregnancy
53111003,Failed attempted abortion with postoperative shock
53881005,Gravida 0
54213000,Oligohydramnios without rupture of membranes
54318006,"Gestation period, 19 weeks"
54559004,Uterine souffle
54844002,Prolapse of gravid uterus
55052008,Diagnostic ultrasound of gravid uterus
55187005,Tarnier's sign
55543007,Previous pregnancies 3
55639004,Failed attempted abortion with laceration of vagina
56160003,Hydrorrhea gravidarum
56313000,Abnormal placenta affecting management of mother
56425003,Placenta edematous
56451001,Failed attempted abortion with perforation of broad ligament
56462001,Failed attempted abortion with soap embolism
57296000,Incarcerated gravid uterus
57576007,Retroverted gravid uterus
57630001,First trimester pregnancy
57907009,"Gestation period, 36 weeks"
58123006,Failed attempted abortion with pulmonary embolism
58532003,Unwanted pregnancy
58881007,Polyp of cervix affecting pregnancy
5939002,Failed attempted abortion complicated by metabolic disorder
59466002,Second trimester pregnancy
59566000,Oligohydramnios
59919008,Failed attempted abortion with complication
60000008,Mesometric pregnancy
60328005,Previous pregnancies 4
60574005,Twin placenta
60755004,Persistent hymen affecting pregnancy
60810003,Quadruplet pregnancy
609133009,Short cervical length in pregnancy
609204004,Subchorionic hematoma
609442008,Antenatal care for woman with history of recurrent miscarriage
609496007,Complication occurring during pregnancy
609525000,Miscarriage of tubal ectopic pregnancy
61452007,Failed attempted abortion with laceration of uterus
61881000,Osiander's sign
61951009,Failed attempted abortion with laceration of periurethral tissue
62131008,Couvelaire uterus
62333002,"Gestation period, 13 weeks"
62531004,Placenta previa marginalis
62612003,Fibrosis of perineum affecting pregnancy
63110000,"Gestation period, 7 weeks"
63503002,"Gestation period, 41 weeks"
63637002,Failed attempted abortion with laceration of broad ligament
63750008,Oblique lie
64181003,Failed attempted abortion with perforation of vagina
64254006,Triplet pregnancy
64920003,"Gestation period, 31 weeks"
65035007,"Gestation period, 22 weeks"
65147003,Twin pregnancy
65483000,Placenta of multiple birth higher than twin
65539006,Impetigo herpetiformis
65683006,"Gestation period, 17 weeks"
65727000,Intrauterine pregnancy
66231000,"Fetal OR intrauterine asphyxia, not clear if noted before OR after onset of labor in liveborn infant"
6678005,"Gestation period, 15 weeks"
66892003,Failed attempted abortion with urinary tract infection
66958002,"Isoimmunization from non-ABO, non-Rh blood-group incompatibility affecting pregnancy"
67042008,Failed attempted abortion complicated by shock
67480003,Indication for care AND/OR intervention in labor AND/OR delivery
67802002,"Malpresentation other than breech, successfully converted to cephalic presentation"
67881007,Annular placenta
68635007,Deficiency of placental function
69217004,Outlet contraction of pelvis
69338007,Kanter's sign
69802008,Cervical cerclage suture present
698632006,Pregnancy induced edema
698708006,Antepartum hemorrhage due to placenta previa type I
698709003,Antepartum hemorrhage due to placenta previa type II
698710008,Antepartum hemorrhage due to placenta previa type III
698711007,Antepartum hemorrhage due to placenta previa type IV
698712000,Antepartum hemorrhage due to cervical polyp
698713005,Antepartum haemorrhage due to cervical erosion
699240001,Combined intrauterine and ovarian pregnancy
699950009,Anti-D isoimmunization affecting pregnancy
700442004,Ultrasonography of fetal ductus venosus
70129008,Placenta accreta
70137000,Deficiency of placental endocrine function
702736005,Supervision of high risk pregnancy with history of previous cesarean section
702737001,Supervision of high risk pregnancy with history of gestational diabetes mellitus
702738006,Supervision of high risk pregnancy
702739003,Supervision of high risk pregnancy with history of previous molar pregnancy
702740001,Supervision of high risk pregnancy with history of previous precipitate labor
702741002,Supervision of high risk pregnancy for multigravida
702742009,Supervision of high risk pregnancy for social problem
702743004,Supervision of high risk pregnancy for multigravida age 15 years or younger
702744005,Supervision of high risk pregnancy for primigravida age 15 years or younger
702985005,Ultrasonography of fetal shunt
70425008,Piskacek's sign
70537007,Hegar's sign
70651004,Calkin's sign
707254000,Amniotic adhesion
710165007,Ultrasonography of fetal head
71028008,Fetal-maternal hemorrhage
710911000000102,Infant feeding antenatal checklist completed
713187004,Polyhydramnios due to maternal disease
713191009,Polyhydramnios due to placental anomaly
713192002,Oligohydramnios due to rupture of membranes
713233004,Supervision of high risk pregnancy with history of previous neonatal death
713234005,Supervision of high risk pregnancy with history of previous intrauterine death
713235006,Supervision of high risk pregnancy with history of previous antepartum hemorrhage
713237003,Supervision of high risk pregnancy with history of previous big baby
713238008,Supervision of high risk pregnancy with history of previous abnormal baby
713239000,Supervision of high risk pregnancy with history of previous primary postpartum hemorrhage
713240003,Supervision of high risk pregnancy with poor obstetric history
713241004,Supervision of high risk pregnancy with history of previous fetal distress
713242006,Supervision of high risk pregnancy with poor reproductive history
713249002,Pyogenic granuloma of gingiva co-occurrent and due to pregnancy
713386003,Supervision of high risk pregnancy for maternal short stature
713387007,Supervision of high risk pregnancy with family history of diabetes mellitus
71355009,"Gestation period, 30 weeks"
713575004,Dizygotic twin pregnancy
713576003,Monozygotic twin pregnancy
716379000,Acute fatty liver of pregnancy
717794008,Supervision of pregnancy with history of infertility
717795009,Supervision of pregnancy with history of insufficient antenatal care
717797001,Antenatal care of elderly primigravida
718475004,Ultrasonography for amniotic fluid index
71848002,Bolt's sign
71901000,Congenital contracted pelvis
72014004,Abnormal fetal duplication
721177006,Injury complicating pregnancy
72161000119100,Antiphospholipid syndrome in pregnancy
723541004,Disease of respiratory system complicating pregnancy
723665008,Vaginal bleeding complicating early pregnancy
72417002,Tumultuous uterine contraction
724483001,Concern about body image related to pregnancy
724486009,Venous disorder co-occurrent with pregnancy
72544005,"Gestation period, 25 weeks"
7266006,Total placenta previa with intrapartum hemorrhage
72846000,"Gestation period, 14 weeks"
72860003,Disorder of amniotic cavity AND/OR membrane
72892002,Normal pregnancy
73161006,Transverse lie
73837001,Failed attempted abortion with cardiac arrest AND/OR failure
74437002,Ahlfeld's sign
74952004,"Gestation period, 3 weeks"
75022004,"Gestational diabetes mellitus, class A>1<"
75094005,Hydrops of placenta
75798003,Twin dichorionic diamniotic placenta
75933004,Threatened abortion in second trimester
76037007,Rigid cervix uteri affecting pregnancy
76380008,Placenta membranacea
76871004,Previous surgery to vagina affecting pregnancy
76889003,Failed attempted abortion with cerebral anoxia
7707000,"Gestation period, 32 weeks"
77186001,Failed attempted abortion with renal tubular necrosis
77278008,Velamentous insertion of umbilical cord
77285007,Placental infarct affecting management of mother
77376005,Gestational edema without hypertension
77386006,Pregnant
7792000,Placenta previa without hemorrhage
7822001,Failed attempted abortion complicated by damage to pelvic organs AND/OR tissues
78395001,"Gestation period, 33 weeks"
786067000,Intramural ectopic pregnancy of myometrium
78789000,Mature abnormal placenta
78808002,Essential hypertension complicating AND/OR reason for care during pregnancy
788290007,Hemangioma of skin in pregnancy
79255005,Mentum presentation of fetus
79290002,Cervical pregnancy
79586000,Tubal pregnancy
79668009,Vasa previa
79992004,"Gestation period, 12 weeks"
80002007,Malpresentation of fetus
80224003,Multiple gestation with one OR more fetal malpresentations
80228000,Lightening of fetus
80256005,Intervillous thrombosis
80487005,"Gestation period, 39 weeks"
8076000,Postmature abnormal placenta
80818002,Previous surgery to perineum AND/OR vulva affecting pregnancy
80847009,Tubal-cornual placenta
80997009,Quintuplet pregnancy
813541000000100,Pregnancy resulting from assisted conception
81521003,Failed attempted abortion with salpingitis
816148008,Disproportion between fetus and pelvis due to conjoined twins
82118009,"Gestation period, 2 weeks"
82661006,Abdominal pregnancy
82664003,Placenta succenturiata
83074005,Unplanned pregnancy
83243004,Rigid pelvic floor affecting pregnancy
833321003,Hydropic degeneration of chorionic villi
83741001,Placenta spuria
83787007,Twin monochorionic diamniotic placenta
84007008,Shock during AND/OR following labor AND/OR delivery
84032005,Halban's sign
840365009,Placenta circummarginate
840625002,Gravid uterus at 12-16 weeks size
840626001,Gravid uterus at 16-20 weeks size
840627005,Gravid uterus at 20-24 weeks size
840628000,Gravid uterus at 24-28 weeks size
840629008,Gravid uterus at 28-32 weeks size
840630003,Gravid uterus at 32-34 weeks size
840631004,Gravid uterus at 34-36 weeks size
840632006,Gravid uterus at 36-38 weeks size
840633001,Gravid uterus at term size
84132007,"Gestation period, 35 weeks"
84235001,Cephalic version
84357006,Twin monochorionic monoamniotic placenta
8445003,Tumor of vulva affecting pregnancy
84693004,Intervillous hemorrhage of placenta
85076009,Placenta fenestrata
855021000000107,Ultrasonography of multiple pregnancy
855031000000109,Doppler ultrasonography of multiple pregnancy
858901000000108,Pregnancy of unknown location
86081009,Herpes gestationis
861281000000109,Antenatal 22 week examination
861301000000105,Antenatal 25 week examination
861321000000101,Antenatal 31 week examination
86203003,Polyhydramnios
86356004,Unstable lie
863897005,Failed attempted termination of pregnancy complicated by acute necrosis of liver
86599005,"Echography, scan B-mode for placental localization"
866481000000104,Ultrasonography to determine estimated date of confinement
86801005,"Gestation period, 6 weeks"
86883006,"Gestation period, 23 weeks"
871005,Contracted pelvis
87178007,"Gestation period, 1 week"
87383005,Maternal distress
87527008,Term pregnancy
87605005,Cornual pregnancy
87621000,Hyperemesis gravidarum before end of 22 week gestation with dehydration
88697005,Papular dermatitis of pregnancy
8884000,Fetal ascites causing disproportion
88895004,Fatigue during pregnancy
89934007,Crowning
904002,Pinard's sign
90645002,Failed attempted abortion without complication
90797000,"Gestation period, 28 weeks"
90968009,Prolonged pregnancy
91162000,Necrosis of liver of pregnancy
9121000119106,Low back pain in pregnancy
91271004,Superfetation
91957002,Back pain complicating pregnancy
92297008,Benign neoplasm of placenta
925561000000100,Gestation less than 28 weeks
92684002,Carcinoma in situ of placenta
9279009,Extra-amniotic pregnancy
9297001,Uterus bicornis affecting pregnancy
931004,"Gestation period, 9 weeks"
9442009,Parturient hemorrhage associated with afibrinogenemia
95606005,Maternal drug exposure
95607001,Maternal drug use
95608006,Necrosis of placenta
9686009,Goodell's sign
9720009,Cicatrix of cervix affecting pregnancy
9780006,Presentation of prolapsed arm of fetus
9864005,Placenta multipartita
9899009,Ovarian pregnancy
"""

# COMMAND ----------

# """Source primis-covid19-vacc-uptake-pregdel.csv """
preg_del = """
code,term
10058006,Miscarriage with amniotic fluid embolism
100801000119107,Maternal tobacco use in pregnancy
10217006,Third degree perineal laceration
10231000132102,In-vitro fertilization pregnancy
102500002,Good neonatal condition at birth
102501003,Well male newborn
102502005,Well female newborn
102503000,Well premature newborn
102504006,Well premature male newborn
102505007,Well premature female newborn
102872000,Pregnancy on oral contraceptive
102875003,Surrogate pregnancy
102876002,Multigravida
102879009,Post-term delivery
102882004,Abnormal placental secretion of chorionic gonadotropin
102885002,Absence of placental secretion of chorionic gonadotropin
102886001,Increased amniotic fluid production
102887005,Decreased amniotic fluid production
102955006,Contraception failure
1031000119109,Insufficient prenatal care
10423003,Braun von Fernwald's sign
10455003,Removal of ectopic cervical pregnancy by evacuation
104851000119103,Postpartum major depression in remission
10573002,Infection of amniotic cavity
106004004,Hemorrhagic complication of pregnancy
106007006,Maternal AND/OR fetal condition affecting labor AND/OR delivery
106008001,Delivery AND/OR maternal condition affecting management
106009009,Fetal condition affecting obstetrical care of mother
106010004,Pelvic dystocia AND/OR uterine disorder
106111002,Clinical sign related to pregnancy
10629511000119102,Rhinitis of pregnancy
10697004,Miscarriage complicated by renal failure
10741871000119101,Alcohol dependence in pregnancy
10743651000119105,Inflammation of cervix in pregnancy
10743831000119100,Neoplasm of uterus affecting pregnancy
10743881000119104,Suspected fetal abnormality affecting management of mother
10745001,Delivery of transverse presentation
10745231000119102,Cardiac arrest due to administration of anesthesia for obstetric procedure in pregnancy
10749691000119103,Obstetric anesthesia with cardiac complication in childbirth
10749811000119108,Obstetric anesthesia with central nervous system complication in childbirth
10749871000119100,Malignant neoplastic disease in pregnancy
10750111000119108,Breast lump in pregnancy
10750161000119106,Cholestasis of pregnancy complicating childbirth
10750411000119102,Nonpurulent mastitis associated with lactation
10750991000119101,Cyst of ovary in pregnancy
10751511000119105,Obstructed labor due to incomplete rotation of fetal head
10751581000119104,Obstructed labor due to disproportion between fetus and pelvis
10751631000119101,Obstructed labor due to abnormality of maternal pelvis
10751701000119102,Spontaneous onset of labor between 37 and 39 weeks gestation with planned cesarean section
10751771000119107,Placental abruption due to afibrinogenemia
10752251000119103,Galactorrhea in pregnancy
10753491000119101,Gestational diabetes mellitus in childbirth
10754331000119108,Air embolism in childbirth
10755951000119102,Heart murmur in mother in childbirth
10756261000119102,Physical abuse complicating childbirth
10756301000119105,Physical abuse complicating pregnancy
10759191000119106,Postpartum septic thrombophlebitis
10759231000119102,Salpingo-oophoritis in pregnancy
10760221000119101,Uterine prolapse in pregnancy
10760261000119106,Uterine laceration during delivery
10760541000119109,Traumatic injury to vulva during pregnancy
10760581000119104,Pain in round ligament in pregnancy
10760661000119109,"Triplets, some live born"
10760701000119102,"Quadruplets, all live born"
10760741000119100,"Quadruplets, some live born"
10760781000119105,"Quintuplets, all live born"
10760821000119100,"Quintuplets, some live born"
10760861000119105,"Sextuplets, all live born"
10760901000119104,"Sextuplets, some live born"
10760981000119107,Psychological abuse complicating pregnancy
10761021000119102,Sexual abuse complicating childbirth
10761061000119107,Sexual abuse complicating pregnancy
10761101000119105,Vacuum assisted vaginal delivery
10761141000119107,Preterm labor in second trimester with preterm delivery in second trimester
10761191000119104,Preterm labor in third trimester with preterm delivery in third trimester
10761241000119104,Preterm labor with preterm delivery
10761341000119105,Preterm labor without delivery
10761391000119102,Tobacco use in mother complicating childbirth
10763001,Therapeutic abortion by insertion of laminaria
1076861000000103,Ultrasonography of retained products of conception
1079101000000101,Antenatal screening shows higher risk of Down syndrome
1079111000000104,Antenatal screening shows lower risk of Down syndrome
1079121000000105,Antenatal screening shows higher risk of Edwards and Patau syndromes
1079131000000107,Antenatal screening shows lower risk of Edwards and Patau syndromes
10807061000119103,Liver disorder in mother complicating childbirth
10808861000119105,Maternal distress during childbirth
10812041000119103,Urinary tract infection due to incomplete miscarriage
10835571000119102,Antepartum hemorrhage due to disseminated intravascular coagulation
10835781000119105,Amniotic fluid embolism in childbirth
10835971000119109,Anti-A sensitization in pregnancy
10836071000119101,Dislocation of symphysis pubis in pregnancy
10836111000119108,Dislocation of symphysis pubis in labor and delivery
109562007,Abnormal chorion
109891004,Detached products of conception
109893001,Detached trophoblast
109894007,Retained placenta
109895008,Retained placental fragment
110081000119109,Bacterial vaginosis in pregnancy
11026009,Miscarriage with renal tubular necrosis
1105781000000106,Misoprostol induction of labour
1105791000000108,Mifepristone induction of labour
1105801000000107,Clitoral tear during delivery
11082009,Abnormal pregnancy
1109951000000101,Pregnancy insufficiently advanced for reliable antenatal screening
1109971000000105,Pregnancy too advanced for reliable antenatal screening
11109001,Miscarriage with uremia
111208003,Melasma gravidarum
111424000,Retained products of conception not following abortion
111431001,Legal abortion complicated by damage to pelvic organs AND/OR tissues
111432008,Legal abortion with air embolism
111447004,Placental condition affecting management of mother
111453004,"Retained placenta, without hemorrhage"
111454005,Retained portions of placenta AND/OR membranes without hemorrhage
111458008,Postpartum venous thrombosis
111459000,"Infection of nipple, associated with childbirth"
112070001,Onset of labor induced
112071002,Postmature pregnancy delivered
112075006,Premature birth of newborn sextuplets
1125006,Sepsis during labor
112925006,Repair of obstetric laceration of vulva
112926007,Suture of obstetric laceration of vagina
112927003,Manual rotation of fetal head
11337002,Quickening of fetus
11373009,Illegal abortion with sepsis
11454006,Failed attempted abortion with amniotic fluid embolism
11466000,Cesarean section
1147951000000100,Delivery following termination of pregnancy
1147961000000102,Breech delivery following termination of pregnancy
1147971000000109,Cephalic delivery following termination of pregnancy
1148801000000108,Monochorionic monoamniotic triplet pregnancy
1148811000000105,Trichorionic triamniotic triplet pregnancy
1148821000000104,Dichorionic triamniotic triplet pregnancy
1148841000000106,Dichorionic diamniotic triplet pregnancy
1149411000000103,Monochorionic diamniotic triplet pregnancy
1149421000000109,Monochorionic triamniotic triplet pregnancy
1167981000000101,Ultrasound scan for chorionicity
11687002,Gestational diabetes mellitus
11718971000119100,Diarrhea in pregnancy
118180006,Finding related to amniotic fluid function
118181005,Finding related to amniotic fluid production
118182003,Finding related to amniotic fluid turnover
118185001,Finding related to pregnancy
118189007,Prenatal finding
118215003,Delivery finding
118216002,Labor finding
11914001,Transverse OR oblique presentation of fetus
11942004,Perineal laceration involving pelvic floor
119901000119109,Hemorrhage co-occurrent and due to partial placenta previa
12062007,Puerperal pelvic cellulitis
12095001,Legal abortion with cardiac arrest AND/OR failure
12296009,Legal abortion with intravascular hemolysis
12349003,Danforth's sign
12394009,Miscarriage with cerebral anoxia
124735008,Late onset of labor
125586008,Disorder of placenta
127009,Miscarriage with laceration of cervix
127363001,"Number of pregnancies, currently pregnant"
127364007,Primigravida
127365008,Gravida 2
127366009,Gravida 3
127367000,Gravida 4
127368005,Gravida 5
127369002,Gravida 6
127370001,Gravida 7
127371002,Gravida 8
127372009,Gravida 9
127373004,Gravida 10
127374005,Gravida more than 10
12803000,High maternal weight gain
128077009,Trauma to vagina during delivery
12867002,Fetal distress affecting management of mother
129597002,Moderate hyperemesis gravidarum
129598007,Severe hyperemesis gravidarum
12983003,Failed attempted abortion with septic shock
130958001,Disorder of placental circulatory function
130959009,Disorder of amniotic fluid turnover
130960004,Disorder of amniotic fluid production
13100007,Legal abortion with laceration of uterus
1323351000000104,First trimester bleeding
13384007,Miscarriage complicated by metabolic disorder
133906008,Postpartum care
133907004,Episiotomy care
13404009,Twin-to-twin blood transfer
1343000,Deep transverse arrest
134435003,Routine antenatal care
134781000119106,High risk pregnancy due to recurrent miscarriage
135881001,Pregnancy review
13763000,"Gestation period, 34 weeks"
13798002,"Gestation period, 38 weeks"
13842006,Sub-involution of uterus
13859001,Premature birth of newborn twins
13866000,Fetal acidemia affecting management of mother
13943000,Failed attempted abortion complicated by embolism
14022007,"Fetal death, affecting management of mother"
14049007,Chaussier's sign
14094001,Excessive vomiting in pregnancy
14119008,Braxton Hicks obstetrical version with extraction
14136000,Miscarriage with perforation of broad ligament
14418008,Precocious pregnancy
1469007,Miscarriage with urinary tract infection
151441000119105,Twin live born in hospital by vaginal delivery
15196006,Intraplacental hematoma
15230009,Liver disorder in pregnancy
1538006,Central nervous system malformation in fetus affecting obstetrical care
15400003,Obstetrical air embolism
15406009,Retromammary abscess associated with childbirth
15413009,High forceps delivery with episiotomy
15467003,"Term birth of identical twins, both living"
15504009,Rupture of gravid uterus
15511008,Legal abortion with parametritis
15539009,Hydrops fetalis due to isoimmunization
15592002,Previous pregnancy 1
156072005,Incomplete miscarriage
156073000,Complete miscarriage
15633004,"Gestation period, 16 weeks"
15643101000119103,Gastroesophageal reflux disease in pregnancy
15663008,Placenta previa centralis
15809008,Miscarriage with perforation of uterus
15898009,Third stage of labor
1592005,Failed attempted abortion with uremia
16038004,Abortion due to Leptospira
161000119101,"Placental abnormality, antepartum"
16101000119107,Incomplete induced termination of pregnancy with complication
16111000119105,Incomplete spontaneous abortion due to hemorrhage
16271000119108,Pyelonephritis in pregnancy
16320551000119109,Bleeding from female genital tract co-occurrent with pregnancy
163403004,On examination - vaginal examination - gravid uterus
163498004,On examination - gravid uterus size
163499007,On examination - fundus 12-16 week size
163500003,On examination - fundus 16-20 week size
163501004,On examination - fundus 20-24 week size
163502006,On examination - fundus 24-28 week size
163504007,On examination - fundus 28-32 week size
163505008,On examination - fundus 32-34 week size
163506009,On examination - fundus 34-36 week size
163507000,On examination - fundus 36-38 week size
163508005,On examination - fundus 38 weeks-term size
163509002,On examination - fundus = term size
163510007,On examination - fundal size = dates
163515002,On examination - oblique lie
163516001,On examination - transverse lie
16356006,Multiple pregnancy
163563004,On examination - vaginal examination - cervical dilatation
163564005,On examination - vaginal examination - cervical os closed
163565006,On examination - vaginal examination - os 0-1 cm dilated
163566007,On examination - vaginal examination - os 1-2cm dilated
163567003,On examination - vaginal examination - os 2-4 cm dilated
163568008,On examination - vaginal examination - os 4-6cm dilated
163569000,On examination - vaginal examination - os 6-8cm dilated
163570004,On examination - vaginal examination - os 8-10cm dilated
163571000,On examination - vaginal examination - os fully dilated
1639007,Abnormality of organs AND/OR soft tissues of pelvis affecting pregnancy
16437531000119105,Ultrasonography for qualitative deepest pocket amniotic fluid volume
164817009,Placental localization
16607004,Missed abortion
16714009,Miscarriage with laceration of bowel
16819009,Delivery of face presentation
16836261000119107,Ectopic pregnancy of left ovary
16836351000119100,Ectopic pregnancy of right ovary
16836391000119105,Pregnancy of left fallopian tube
16836431000119100,Ruptured tubal pregnancy of right fallopian tube
16836571000119103,Ruptured tubal pregnancy of left fallopian tube
16836891000119104,Pregnancy of right fallopian tube
169224002,Ultrasound scan for fetal cephalometry
169225001,Ultrasound scan for fetal maturity
169228004,Ultrasound scan for fetal presentation
169229007,Dating/booking ultrasound scan
169230002,Ultrasound scan for fetal viability
169470007,Combined oral contraceptive pill failure
169471006,Progestogen-only pill failure
169488004,Contraceptive intrauterine device failure - pregnant
16950007,Fourth degree perineal laceration involving anal mucosa
169501005,"Pregnant, diaphragm failure"
169508004,"Pregnant, sheath failure"
169524003,Depot contraceptive failure
169533001,Contraceptive sponge failure
169539002,Symptothermal contraception failure
169544009,Postcoital oral contraceptive pill failure
169548007,Vasectomy failure
169550004,Female sterilization failure
169560008,Pregnant - urine test confirms
169561007,Pregnant - blood test confirms
169562000,Pregnant - vaginal examination confirms
169563005,Pregnant - on history
169564004,Pregnant - on abdominal palpation
169565003,Pregnant - planned
169566002,Pregnancy unplanned but wanted
169567006,Pregnancy unplanned and unwanted
169568001,Unplanned pregnancy unknown if child is wanted
169569009,Questionable if pregnancy was planned
169572002,Antenatal care categorized by gravida number
169573007,Antenatal care of primigravida
169574001,Antenatal care of second pregnancy
169575000,Antenatal care of third pregnancy
169576004,Antenatal care of multipara
169578003,Antenatal care: obstetric risk
169579006,Antenatal care: uncertain dates
169582001,Antenatal care: history of stillbirth
169583006,Antenatal care: history of perinatal death
169584000,Antenatal care: poor obstetric history
169585004,Antenatal care: history of trophoblastic disease
169587007,Antenatal care: precious pregnancy
169588002,Antenatal care: elderly primiparous
169589005,Antenatal care: history of infertility
169591002,Antenatal care: social risk
169595006,Antenatal care: history of child abuse
169597003,Antenatal care: medical risk
169598008,Antenatal care: gynecological risk
169600002,Antenatal care: under 5ft tall
169602005,Antenatal care: 10 years plus since last pregnancy
169603000,"Antenatal care: primiparous, under 17 years"
169604006,"Antenatal care: primiparous, older than 30 years"
169605007,"Antenatal care: multiparous, older than 35 years"
169613008,Antenatal care provider
169614002,Antenatal care from general practitioner
169615001,Antenatal care from consultant
169616000,Antenatal - shared care
169620001,Delivery: no place booked
169628008,Delivery booking - length of stay
169646002,Antenatal amniocentesis
169650009,Antenatal amniocentesis wanted
169651008,Antenatal amniocentesis - awaited
169652001,Antenatal amniocentesis - normal
169653006,Antenatal amniocentesis - abnormal
169657007,Antenatal ultrasound scan status
169661001,Antenatal ultrasound scan wanted
169662008,Antenatal ultrasound scan awaited
169667002,Antenatal ultrasound scan for slow growth
169668007,Antenatal ultrasound scan 4-8 weeks
169669004,Antenatal ultrasound scan at 9-16 weeks
169670003,Antenatal ultrasound scan at 17-22 weeks
169711001,Antenatal booking examination
169712008,Antenatal 12 weeks examination
169713003,Antenatal 16 week examination
169714009,Antenatal 20 week examination
169715005,Antenatal 24 week examination
169716006,Antenatal 28 week examination
169717002,Antenatal 30 week examination
169718007,Antenatal 32 week examination
169719004,Antenatal 34 week examination
169720005,Antenatal 35 week examination
169721009,Antenatal 36 week examination
169722002,Antenatal 37 week examination
169723007,Antenatal 38 week examination
169724001,Antenatal 39 week examination
169725000,Antenatal 40 week examination
169726004,Antenatal 41 week examination
169727008,Antenatal 42 week examination
169762003,Postnatal visit
169763008,Postnatal - first day visit
169764002,Postnatal - second day visit
169765001,Postnatal - third day visit
169766000,Postnatal - fourth day visit
169767009,Postnatal - fifth day visit
169768004,Postnatal - sixth day visit
169769007,Postnatal - seventh day visit
169770008,Postnatal - eighth day visit
169771007,Postnatal - ninth day visit
169772000,Postnatal - tenth day visit
169826009,Single live birth
169827000,Single stillbirth
169828005,Twins - both live born
169829002,Twins - one still and one live born
169830007,Twins - both stillborn
169831006,Triplets - all live born
169832004,Triplets - two live and one stillborn
169833009,Triplets - one live and two stillborn
169834003,Triplets - three stillborn
169836001,Birth of child
169952004,Placental finding
169954003,Placenta normal O/E
169960003,Labor details
169961004,Normal birth
17285009,Intraperitoneal pregnancy
173300003,Disorder of pregnancy
17333005,Term birth of multiple newborns
17335003,Illegal abortion with laceration of bowel
17369002,Miscarriage
17380002,Failed attempted abortion with acute renal failure
17382005,Cervical incompetence
17433009,Ruptured ectopic pregnancy
17532001,Breech malpresentation successfully converted to cephalic presentation
17588005,Abnormal yolk sac
17594002,Fetal bradycardia affecting management of mother
176827002,Dilation of cervix uteri and vacuum aspiration of products of conception from uterus
176849008,Introduction of abortifacient into uterine cavity
176854004,Insertion of prostaglandin abortifacient suppository
177128002,Induction and delivery procedures
177129005,Surgical induction of labor
177135005,Oxytocin induction of labor
177136006,Prostaglandin induction of labor
177141003,Elective cesarean section
177142005,Elective upper segment cesarean section
177143000,Elective lower segment cesarean section
177152009,Breech extraction delivery with version
177157003,Spontaneous breech delivery
177158008,Assisted breech delivery
177161009,Forceps cephalic delivery
177162002,High forceps cephalic delivery with rotation
177164001,Midforceps cephalic delivery with rotation
177167008,Barton forceps cephalic delivery with rotation
177168003,DeLee forceps cephalic delivery with rotation
177170007,Piper forceps delivery
177173009,High vacuum delivery
177174003,Low vacuum delivery
177175002,Vacuum delivery before full dilation of cervix
177176001,Trial of vacuum delivery
177179008,Cephalic vaginal delivery with abnormal presentation of head at delivery without instrument
177180006,Manipulative cephalic vaginal delivery with abnormal presentation of head at delivery without instrument
177181005,Non-manipulative cephalic vaginal delivery with abnormal presentation of head at delivery without instrument
177184002,Normal delivery procedure
177185001,Water birth delivery
177200004,Instrumental removal of products of conception from delivered uterus
177203002,Manual removal of products of conception from delivered uterus
177204008,Manual removal of placenta from delivered uterus
177208006,Repositioning of inverted uterus
177212000,Normal delivery of placenta
177217006,Immediate repair of obstetric laceration
177218001,Immediate repair of obstetric laceration of uterus or cervix uteri
177219009,Immediate repair of obstetric laceration of perineum and sphincter of anus
177220003,Immediate repair of obstetric laceration of vagina and floor of pelvis
177221004,Immediate repair of minor obstetric laceration
177227000,Secondary repair of obstetric laceration
17787002,Peripheral neuritis in pregnancy
178280004,Postnatal infection
17860005,Low forceps delivery with episiotomy
18114009,Prenatal examination and care of mother
18122002,Normal uterine contraction wave
18237006,Parenchymatous mastitis associated with childbirth
18260003,Postpartum psychosis
18302006,Therapeutic abortion by hysterotomy
18391007,Elective abortion
184339009,Miscarriage at 8 to 28 weeks
18606002,Dyscoordinate labor
18613002,Miscarriage with septic shock
18625004,Low forceps delivery
18684002,Illegal abortion complicated by metabolic disorder
18872002,Illegal abortion with laceration of cervix
18894005,Legal abortion with cerebral anoxia
19099008,Failed attempted abortion with laceration of bladder
19169002,Miscarriage in first trimester
19228003,Failed attempted abortion with perforation of bladder
19363005,Failed attempted abortion with blood-clot embolism
19390001,Partial breech delivery with forceps to aftercoming head
195005,Illegal abortion with endometritis
19564003,Illegal abortion with laceration of periurethral tissue
19569008,Mild hyperemesis gravidarum
19729005,Prolapse of anterior lip of cervix obstructing labor
19773009,Infection of the breast AND/OR nipple associated with childbirth
198332009,Old uterine laceration due to obstetric cause
198347000,Cicatrix (postpartum) of cervix
198617006,Delivery of viable fetus in abdominal pregnancy
198620003,Ruptured tubal pregnancy
198624007,Membranous pregnancy
198626009,Mesenteric pregnancy
198627000,Angular pregnancy
198644001,Incomplete miscarriage with genital tract or pelvic infection
198645000,Incomplete miscarriage with delayed or excessive hemorrhage
198646004,Incomplete miscarriage with damage to pelvic organs or tissues
198647008,Incomplete miscarriage with renal failure
198648003,Incomplete miscarriage with metabolic disorder
198649006,Incomplete miscarriage with shock
198650006,Incomplete miscarriage with embolism
198655001,Complete miscarriage with genital tract or pelvic infection
198656000,Complete miscarriage with delayed or excessive hemorrhage
198657009,Complete miscarriage with damage to pelvic organs or tissues
198659007,Complete miscarriage with renal failure
198660002,Complete miscarriage with metabolic disorder
19866007,Previous operation to cervix affecting pregnancy
198661003,Complete miscarriage with shock
198663000,Complete miscarriage with embolism
198705001,Incomplete legal abortion with genital tract or pelvic infection
198706000,Incomplete legal abortion with delayed or excessive hemorrhage
198707009,Incomplete legal abortion with damage to pelvic organs or tissues
198708004,Incomplete legal abortion with renal failure
198709007,Incomplete legal abortion with metabolic disorder
198710002,Incomplete legal abortion with shock
198711003,Incomplete legal abortion with embolism
198718009,Complete legal abortion with genital tract or pelvic infection
198719001,Complete legal abortion with delayed or excessive hemorrhage
198720007,Complete legal abortion with damage to pelvic organs or tissues
198721006,Complete legal abortion with renal failure
198722004,Complete legal abortion with metabolic disorder
198723009,Complete legal abortion with shock
198724003,Complete legal abortion with embolism
198743003,Illegal abortion incomplete
198744009,Incomplete illegal abortion with genital tract or pelvic infection
198745005,Incomplete illegal abortion with delayed or excessive hemorrhage
198746006,Incomplete illegal abortion with damage to pelvic organs or tissues
198747002,Incomplete illegal abortion with renal failure
198748007,Incomplete illegal abortion with metabolic disorder
198749004,Incomplete illegal abortion with shock
198750004,Incomplete illegal abortion with embolism
198755009,Illegal abortion complete
198757001,Complete illegal abortion with delayed or excessive hemorrhage
198758006,Complete illegal abortion with damage to pelvic organs or tissues
198759003,Complete illegal abortion with renal failure
198760008,Complete illegal abortion with metabolic disorder
198761007,Complete illegal abortion with shock
198762000,Complete illegal abortion with embolism
198806007,Failed attempted abortion with genital tract or pelvic infection
198807003,Failed attempted abortion with delayed or excessive hemorrhage
198808008,Failed attempted abortion with damage to pelvic organs or tissues
198809000,Failed attempted abortion with renal failure
198810005,Failed attempted abortion with metabolic disorder
198811009,Failed attempted abortion with shock
198812002,Failed attempted abortion with embolism
198832001,Damage to pelvic organs or tissues following abortive pregnancy
198861000,"Readmission for retained products of conception, spontaneous abortion"
198862007,"Readmission for retained products of conception, legal abortion"
198863002,"Readmission for retained products of conception, illegal abortion"
198874006,"Failed medical abortion, complicated by genital tract and pelvic infection"
198875007,"Failed medical abortion, complicated by delayed or excessive hemorrhage"
198876008,"Failed medical abortion, complicated by embolism"
198878009,"Failed medical abortion, without complication"
198899007,Placenta previa without hemorrhage - delivered
198900002,Placenta previa without hemorrhage - not delivered
198903000,Placenta previa with hemorrhage
198905007,Placenta previa with hemorrhage - delivered
198906008,Placenta previa with hemorrhage - not delivered
198910006,Placental abruption - delivered
198911005,Placental abruption - not delivered
198912003,Premature separation of placenta with coagulation defect
198917009,Antepartum hemorrhage with coagulation defect - delivered
198918004,Antepartum hemorrhage with coagulation defect - not delivered
198920001,Antepartum hemorrhage with trauma
198922009,Antepartum hemorrhage with trauma - delivered
198923004,Antepartum hemorrhage with trauma - not delivered
198925006,Antepartum hemorrhage with uterine leiomyoma
198927003,Antepartum hemorrhage with uterine leiomyoma - delivered
198928008,Antepartum hemorrhage with uterine leiomyoma - not delivered
198991006,Eclampsia - delivered with postnatal complication
198992004,Eclampsia in pregnancy
198993009,Eclampsia with postnatal complication
199022003,Mild hyperemesis-delivered
199023008,Mild hyperemesis-not delivered
199025001,Hyperemesis gravidarum with metabolic disturbance
199027009,Hyperemesis gravidarum with metabolic disturbance - delivered
199028004,Hyperemesis gravidarum with metabolic disturbance - not delivered
199032005,Late pregnancy vomiting - delivered
199063009,Post-term pregnancy - delivered
199064003,Post-term pregnancy - not delivered
199087006,Habitual aborter - delivered
199088001,Habitual aborter - not delivered
199093003,Peripheral neuritis in pregnancy - delivered
199095005,Peripheral neuritis in pregnancy - not delivered
199096006,Peripheral neuritis in pregnancy with postnatal complication
199099004,Asymptomatic bacteriuria in pregnancy - delivered
199100007,Asymptomatic bacteriuria in pregnancy - delivered with postnatal complication
199101006,Asymptomatic bacteriuria in pregnancy - not delivered
199102004,Asymptomatic bacteriuria in pregnancy with postnatal complication
199110003,Infections of kidney in pregnancy
199112006,Infections of the genital tract in pregnancy
199117000,Liver disorder in pregnancy - delivered
199118005,Liver disorder in pregnancy - not delivered
199121007,Fatigue during pregnancy - delivered
199122000,Fatigue during pregnancy - delivered with postnatal complication
199123005,Fatigue during pregnancy - not delivered
199124004,Fatigue during pregnancy with postnatal complication
199127006,Herpes gestationis - delivered
199128001,Herpes gestationis - delivered with postnatal complication
199129009,Herpes gestationis - not delivered
199139003,Pregnancy-induced edema and proteinuria without hypertension
199141002,Gestational edema with proteinuria
199192005,Maternal rubella during pregnancy - baby delivered
199194006,Maternal rubella during pregnancy - baby not yet delivered
199225007,Diabetes mellitus during pregnancy - baby delivered
199227004,Diabetes mellitus during pregnancy - baby not yet delivered
199244000,Anemia during pregnancy - baby delivered
199246003,Anemia during pregnancy - baby not yet delivered
199252002,Drug dependence during pregnancy - baby delivered
199254001,Drug dependence during pregnancy - baby not yet delivered
199266007,Congenital cardiovascular disorder during pregnancy - baby delivered
199305006,Complications specific to multiple gestation
199306007,Continuing pregnancy after abortion of one fetus or more
199307003,Continuing pregnancy after intrauterine death of one or more fetuses
199314001,Normal delivery but ante- or post- natal conditions present
199317008,Twin pregnancy - delivered
199318003,Twin pregnancy with antenatal problem
199321001,Triplet pregnancy - delivered
199322008,Triplet pregnancy with antenatal problem
199325005,Quadruplet pregnancy - delivered
199326006,Quadruplet pregnancy with antenatal problem
199329004,"Multiple delivery, all spontaneous"
199330009,"Multiple delivery, all by forceps and vacuum extractor"
199331008,"Multiple delivery, all by cesarean section"
199344003,Unstable lie - delivered
199345002,Unstable lie with antenatal problem
199358001,Oblique lie - delivered
199359009,Oblique lie with antenatal problem
199362007,Transverse lie - delivered
199363002,Transverse lie with antenatal problem
199375007,High head at term - delivered
199378009,Multiple pregnancy with malpresentation
199380003,Multiple pregnancy with malpresentation - delivered
199381004,Multiple pregnancy with malpresentation with antenatal problem
199384007,Prolapsed arm - delivered
199385008,Prolapsed arm with antenatal problem
199397009,Cephalopelvic disproportion
199405005,Generally contracted pelvis - delivered
199406006,Generally contracted pelvis with antenatal problem
199409004,Inlet pelvic contraction - delivered
199410009,Inlet pelvic contraction with antenatal problem
199413006,Outlet pelvic contraction - delivered
199414000,Outlet pelvic contraction with antenatal problem
199416003,Mixed feto-pelvic disproportion
199418002,Mixed feto-pelvic disproportion - delivered
199419005,Mixed feto-pelvic disproportion with antenatal problem
199422007,Large fetus causing disproportion - delivered
199423002,Large fetus causing disproportion with antenatal problem
199425009,Hydrocephalic disproportion
199427001,Hydrocephalic disproportion - delivered
199428006,Hydrocephalic disproportion with antenatal problem
199466009,Retroverted incarcerated gravid uterus
199468005,Retroverted incarcerated gravid uterus - delivered
199469002,Retroverted incarcerated gravid uterus - delivered with postnatal complication
199470001,Retroverted incarcerated gravid uterus with antenatal problem
199471002,Retroverted incarcerated gravid uterus with postnatal complication
199482005,Cervical incompetence - delivered
199483000,Cervical incompetence - delivered with postnatal complication
199484006,Cervical incompetence with antenatal problem
199485007,Cervical incompetence with postnatal complication
199577000,Fetal-maternal hemorrhage - delivered
199578005,Fetal-maternal hemorrhage with antenatal problem
199582007,Rhesus isoimmunization - delivered
199583002,Rhesus isoimmunization with antenatal problem
199595002,Labor and delivery complication by meconium in amniotic fluid
199596001,Labor and delivery complicated by fetal heart rate anomaly with meconium in amniotic fluid
199597005,Labor and delivery complicated by biochemical evidence of fetal stress
199625002,Placental transfusion syndromes
199646006,Polyhydramnios - delivered
199647002,Polyhydramnios with antenatal problem
199653002,Oligohydramnios - delivered
199654008,Oligohydramnios with antenatal problem
199677008,Amniotic cavity infection - delivered
199678003,Amniotic cavity infection with antenatal problem
199694005,Failed mechanical induction - delivered
199695006,Failed mechanical induction with antenatal problem
199710008,"Sepsis during labor, delivered"
199711007,Sepsis during labor with antenatal problem
199718001,Elderly primigravida - delivered
199719009,Elderly primigravida with antenatal problem
199732004,Abnormal findings on antenatal screening of mother
199733009,Abnormal hematologic finding on antenatal screening of mother
199734003,Abnormal biochemical finding on antenatal screening of mother
199735002,Abnormal cytological finding on antenatal screening of mother
199737005,Abnormal radiological finding on antenatal screening of mother
199738000,Abnormal chromosomal and genetic finding on antenatal screening of mother
199741009,Malnutrition in pregnancy
199745000,Complication occurring during labor and delivery
199746004,Obstructed labor
199747008,Obstructed labor due to fetal malposition
199749006,Obstructed labor due to fetal malposition - delivered
199750006,Obstructed labor due to fetal malposition with antenatal problem
199751005,Obstructed labor due to breech presentation
199752003,Obstructed labor due to face presentation
199753008,Obstructed labor due to brow presentation
199754002,Obstructed labor due to shoulder presentation
199755001,Obstructed labor due to compound presentation
199757009,Obstructed labor caused by bony pelvis
199759007,Obstructed labor caused by bony pelvis - delivered
199760002,Obstructed labor caused by bony pelvis with antenatal problem
199761003,Obstructed labor due to deformed pelvis
199762005,Obstructed labor due to generally contracted pelvis
199763000,Obstructed labor due to pelvic inlet contraction
199764006,Obstructed labor due to pelvic outlet and mid-cavity contraction
199765007,Obstructed labor due to abnormality of maternal pelvic organs
199767004,Obstructed labor caused by pelvic soft tissues
199769001,Obstructed labor caused by pelvic soft tissues - delivered
199770000,Obstructed labor caused by pelvic soft tissues with antenatal problem
199774009,Deep transverse arrest - delivered
199775005,Deep transverse arrest with antenatal problem
199783004,Shoulder dystocia - delivered
199784005,Shoulder dystocia with antenatal problem
199787003,Locked twins - delivered
199788008,Locked twins with antenatal problem
199806003,Obstructed labor due to unusually large fetus
199819004,Primary uterine inertia - delivered
199821009,Primary uterine inertia with antenatal problem
199824001,Secondary uterine inertia - delivered
199825000,Secondary uterine inertia with antenatal problem
199833004,Precipitate labor - delivered
199834005,Precipitate labor with antenatal problem
199838008,Hypertonic uterine inertia - delivered
199839000,Hypertonic uterine inertia with antenatal problem
199847000,Prolonged first stage - delivered
199848005,Prolonged first stage with antenatal problem
199857004,Prolonged second stage - delivered
199858009,Prolonged second stage with antenatal problem
199860006,"Delayed delivery of second twin, triplet etc"
199862003,Delayed delivery second twin - delivered
199863008,Delayed delivery second twin with antenatal problem
199889008,Short cord - delivered
199890004,Short cord with antenatal problem
199916005,First degree perineal tear during delivery - delivered
199917001,First degree perineal tear during delivery with postnatal problem
199925004,Second degree perineal tear during delivery - delivered
199926003,Second degree perineal tear during delivery with postnatal problem
199930000,Third degree perineal tear during delivery - delivered
199931001,Third degree perineal tear during delivery with postnatal problem
199934009,Fourth degree perineal tear during delivery - delivered
199935005,Fourth degree perineal tear during delivery with postnatal problem
199958008,Ruptured uterus before labor
199960005,Rupture of uterus before labor - delivered
199961009,Rupture of uterus before labor with antenatal problem
199964001,Rupture of uterus during and after labor - delivered
199965000,Rupture of uterus during and after labor - delivered with postnatal problem
199969006,Obstetric inversion of uterus - delivered with postnatal problem
199970007,Obstetric inversion of uterus with postnatal problem
199972004,Laceration of cervix - obstetric
199974003,Obstetric laceration of cervix - delivered
199975002,Obstetric laceration of cervix with postnatal problem
199977005,Obstetric high vaginal laceration
199979008,Obstetric high vaginal laceration - delivered
199980006,Obstetric high vaginal laceration with postnatal problem
199990003,Obstetric damage to pelvic joints and ligaments - delivered
199991004,Obstetric damage to pelvic joints and ligaments with postnatal problem
199997000,Obstetric pelvic hematoma with postnatal problem
200025008,Secondary postpartum hemorrhage - delivered with postnatal problem
200030007,Postpartum coagulation defects - delivered with postnatal problem
200031006,Postpartum coagulation defects with postnatal problem
200038000,Retained placenta with no hemorrhage with postnatal problem
200040005,Retained portion of placenta or membranes with no hemorrhage
200043007,Retained products with no hemorrhage with postnatal problem
200046004,Complications of anesthesia during labor and delivery
200049006,Obstetric anesthesia with pulmonary complications - delivered
200050006,Obstetric anesthesia with pulmonary complications - delivered with postnatal problem
200052003,Obstetric anesthesia with pulmonary complications with postnatal problem
200054002,Obstetric anesthesia with cardiac complications
200056000,Obstetric anesthesia with cardiac complications - delivered
200057009,Obstetric anesthesia with cardiac complications - delivered with postnatal problem
200059007,Obstetric anesthesia with cardiac complications with postnatal problem
200061003,Obstetric anesthesia with central nervous system complications
200063000,Obstetric anesthesia with central nervous system complications - delivered
200064006,Obstetric anesthesia with central nervous system complication - delivered with postnatal problem
200066008,Obstetric anesthesia with central nervous system complication with postnatal problem
200068009,Obstetric toxic reaction caused by local anesthesia
200070000,Toxic reaction caused by local anesthesia during puerperium
200075005,Toxic reaction caused by local anesthetic during labor and delivery
200077002,Cardiac complications of anesthesia during labor and delivery
200099005,Maternal distress - delivered
200100002,Maternal distress - delivered with postnatal problem
200102005,Maternal distress with postnatal problem
200105007,Obstetric shock - delivered
200106008,Obstetric shock - delivered with postnatal problem
200108009,Obstetric shock with postnatal problem
200111005,Maternal hypotension syndrome - delivered
200112003,Maternal hypotension syndrome - delivered with postnatal problem
200114002,Maternal hypotension syndrome with postnatal problem
200118004,Post-delivery acute renal failure with postnatal problem
200125006,Infection of obstetric surgical wound
200130005,Forceps delivery - delivered
200133007,Delivery by combination of forceps and vacuum extractor
200134001,Delivered by mid-cavity forceps with rotation
200138003,Vacuum extractor delivery - delivered
200142000,Breech extraction - delivered
200144004,Deliveries by cesarean
200146002,Cesarean delivery - delivered
200147006,Cesarean section - pregnancy at term
200148001,Delivery by elective cesarean section
200149009,Delivery by emergency cesarean section
200150009,Delivery by cesarean hysterectomy
200151008,Cesarean section following previous cesarean section
200154000,Deliveries by destructive operation
200164009,Exhaustion during labor
200173001,Intrapartum hemorrhage with coagulation defect
200181000,Puerperal endometritis - delivered with postnatal complication
200182007,Puerperal endometritis with postnatal complication
200185009,Puerperal salpingitis - delivered with postnatal complication
200187001,Puerperal salpingitis with postnatal complication
200190007,Puerperal peritonitis - delivered with postnatal complication
200191006,Puerperal peritonitis with postnatal complication
200195002,Puerperal septicemia - delivered with postnatal complication
200196001,Puerperal sepsis with postnatal complication
200237000,Postnatal deep vein thrombosis - delivered with postnatal complication
200238005,Postnatal deep vein thrombosis with postnatal complication
200255009,Hemorrhoids in the puerperium
200277008,Puerperal pyrexia of unknown origin
200280009,Puerperal pyrexia of unknown origin - delivered with postnatal complication
200281008,Puerperal pyrexia of unknown origin with postnatal complication
200286003,Obstetric air pulmonary embolism
200288002,Obstetric air pulmonary embolism - delivered
200289005,Obstetric air pulmonary embolism - delivered with postnatal complication
200290001,Obstetric air pulmonary embolism with antenatal complication
200291002,Obstetric air pulmonary embolism with postnatal complication
200297003,Amniotic fluid pulmonary embolism with postnatal complication
200330000,Puerperal cerebrovascular disorder - delivered
200331001,Puerperal cerebrovascular disorder - delivered with postnatal complication
200332008,Puerperal cerebrovascular disorder with antenatal complication
200333003,Puerperal cerebrovascular disorder with postnatal complication
200337002,Cesarean wound disruption - delivered with postnatal complication
200338007,Cesarean wound disruption with postnatal complication
200342005,Obstetric perineal wound disruption - delivered with postnatal complication
200343000,Obstetric perineal wound disruption with postnatal complication
200351002,Placental polyp - delivered with postnatal complication
200352009,Placental polyp with postnatal complication
200444007,Galactorrhea in pregnancy and the puerperium
200446009,Galactorrhea in pregnancy and the puerperium - delivered
200447000,Galactorrhea in pregnancy and the puerperium - delivered with postnatal complication
200448005,Galactorrhea in pregnancy and the puerperium with antenatal complication
200449002,Galactorrhea in pregnancy and the puerperium with postnatal complication
201134009,Alopecia of pregnancy
20216003,Legal abortion with perforation of uterus
20236002,Labor established
20259008,Finding of ballottement of fetal parts
20272009,Premature birth of multiple newborns
20286008,Gorissenne's sign
20391007,Amniotic cyst
20483002,Legal abortion with renal tubular necrosis
206057002,Fetal or neonatal effect of transverse lie before labor
206070006,Fetal or neonatal effect of placental damage caused by cesarean section
206112008,Fetal or neonatal effect of maternal bony pelvis abnormality during labor and delivery
206113003,Fetal or neonatal effect of maternal contracted pelvis during labor and delivery
206114009,Fetal or neonatal effect of persistent occipito-posterior malposition during labor and delivery
206115005,Fetal or neonatal effect of shoulder presentation during labor and delivery
206116006,Fetal or neonatal effect of transverse lie during labor and delivery
206117002,Fetal or neonatal effect of face presentation during labor and delivery
206118007,Fetal or neonatal effect of disproportion during labor and delivery
206121009,Fetal or neonatal effect of forceps delivery
206122002,Fetal or neonatal effect of vacuum extraction delivery
206123007,Fetal or neonatal effect of cesarean section
206134005,Fetal or neonatal effect of precipitate delivery
206137003,Fetal or neonatal effect of maternal contraction ring
206138008,Fetal or neonatal effect of hypertonic labor
206139000,Fetal or neonatal effect of hypertonic uterine dysfunction
206146009,Fetal or neonatal effect of induction of labor
206148005,Fetal or neonatal effect of long labor
206149002,Fetal or neonatal effect of instrumental delivery
206242003,Liver subcapsular hematoma due to birth trauma
206244002,Vulval hematoma due to birth trauma
20625004,Obstruction caused by position of fetus at onset of labor
206365006,Clostridial intra-amniotic fetal infection
206538000,Idiopathic hydrops fetalis
20753005,Hypertensive heart disease complicating AND/OR reason for care during pregnancy
20845005,Meconium in amniotic fluid affecting management of mother
20932005,Puerperal salpingitis
21127004,Term birth of newborn male
21243004,Term birth of newborn
21280005,Legal abortion with postoperative shock
21334005,Failed attempted abortion with oliguria
21346009,Double uterus affecting pregnancy
21360006,Miscarriage with afibrinogenemia
21504004,Pressure collapse of lung after anesthesia AND/OR sedation in labor AND/OR delivery
21604008,Illegal abortion with blood-clot embolism
21623001,Fetal biophysical profile
21737000,Retained fetal tissue
21987001,Delayed delivery of second of multiple births
22046002,Illegal abortion with salpingitis
22173004,Excessive fetal growth affecting management of mother
22271007,Abnormal fetal heart beat first noted during labor AND/OR delivery in liveborn infant
22281000119101,Post-term pregnancy of 40 to 42 weeks
22288000,Tumor of cervix affecting pregnancy
223003,Tumor of body of uterus affecting pregnancy
2239004,Previous pregnancies 6
22399000,Puerperal endometritis
22514005,"Term birth of fraternal twins, one living, one stillborn"
225245001,Rubbing up a contraction
22633006,"Vaginal delivery, medical personnel present"
22753004,Fetal AND/OR placental disorder affecting management of mother
22758008,Spalding-Horner sign
22846003,Hepatorenal syndrome following delivery
228471000000102,Routine obstetric scan
228531000000103,Cervical length scanning at 24 weeks
228551000000105,Mid trimester scan
228691000000101,Non routine obstetric scan for fetal observations
228701000000101,Fetal ascites scan
228711000000104,Rhesus detailed scan
22879003,Previous pregnancies 2
228881000000102,Ultrasound monitoring of early pregnancy
228921000000108,Obstetric ultrasound monitoring
22966008,Hypertensive heart AND renal disease complicating AND/OR reason for care during pregnancy
229761000000107,Vacuum aspiration of products of conception from uterus using flexible cannula
229801000000102,Vacuum aspiration of products of conception from uterus using rigid cannula
23001005,Stenosis AND/OR stricture of cervix affecting pregnancy
23128002,"Cervical dilatation, 5cm"
23171006,Delayed AND/OR secondary postpartum hemorrhage
23177005,Crepitus uteri
232671000000100,Fetal measurement scan
23332002,Failed trial of labor
23401002,Illegal abortion with perforation of bowel
234058009,Genital varices in pregnancy
23431000119106,Rupture of uterus during labor
234380002,Kell isoimmunization of the newborn
23464008,"Gestation period, 20 weeks"
23508005,"Uterine incoordination, first degree"
235888006,Cholestasis of pregnancy
23652008,"Cervical dilatation, 3cm"
23667007,Term pregnancy delivered
236883005,Evacuation of retained product of conception
236958009,Induction of labor
236971007,Stabilizing induction
236973005,Delivery procedure
236974004,Instrumental delivery
236975003,Nonrotational forceps delivery
236976002,Outlet forceps delivery
236977006,"Forceps delivery, face to pubes"
236978001,Forceps delivery to the aftercoming head
236979009,Pajot's maneuver
236980007,Groin traction at breech delivery
236981006,Lovset's maneuver
236982004,Delivery of the after coming head
236983009,Burns Marshall maneuver
236984003,Mauriceau Smellie Veit maneuver
236985002,Emergency lower segment cesarean section
236986001,Emergency upper segment cesarean section
236987005,Emergency cesarean hysterectomy
236988000,Elective cesarean hysterectomy
236989008,Abdominal delivery for shoulder dystocia
236990004,Postmortem cesarean section
236991000,Operation to facilitate delivery
236992007,Right mediolateral episiotomy
236994008,Placental delivery procedure
237000000,Modification of uterine activity
237001001,Augmentation of labor
237002008,Stimulation of labor
237003003,Tocolysis for hypertonicity of uterus
237004009,Prophylactic oxytocic administration
237005005,Manual procedure for malpresentation or position
237006006,Internal conversion of face to vertex
237007002,Reposition of a prolapsed arm
237008007,Maneuvers for delivery in shoulder dystocia
237009004,McRoberts maneuver
237010009,Suprapubic pressure on fetal shoulder
237011008,Wood's screw maneuver
237012001,Freeing the posterior arm
237014000,Postpartum obstetric operation
237015004,Surgical control of postpartum hemorrhage
237021000,Hydrostatic replacement of inverted uterus
237022007,Manual replacement of inverted uterus
237023002,Haultain's operation
237024008,Kustner's operation
23717007,Benign essential hypertension complicating AND/OR reason for care during pregnancy
237198001,Rectocele complicating postpartum care - baby delivered during previous episode of care
237201006,Rectocele complicating antenatal care - baby not yet delivered
237202004,Rectocele - delivered with postpartum complication
237205002,Rectocele affecting obstetric care
237206001,Rectocele - baby delivered
237230004,Uremia in pregnancy without hypertension
237233002,Concealed pregnancy
237234008,Undiagnosed pregnancy
237235009,Undiagnosed breech
237236005,Undiagnosed multiple pregnancy
237237001,Undiagnosed twin
237238006,Pregnancy with uncertain dates
237239003,Low risk pregnancy
237240001,Teenage pregnancy
237241002,Viable pregnancy
237242009,Non-viable pregnancy
237243004,Biochemical pregnancy
237244005,Single pregnancy
237247003,Continuing pregnancy after abortion of sibling fetus
237249000,Complete hydatidiform mole
237250000,Incomplete hydatidiform mole
237252008,Placental site trophoblastic tumor
237253003,Viable fetus in abdominal pregnancy
237254009,Unruptured tubal pregnancy
237256006,Disorder of pelvic size and disproportion
237257002,Midpelvic contraction
237259004,Variation of placental position
237261008,Long umbilical cord
237270006,Antepartum hemorrhage with hypofibrinogenemia
237271005,Antepartum hemorrhage with hyperfibrinolysis
237272003,Antepartum hemorrhage with afibrinogenemia
237273008,Revealed accidental hemorrhage
237274002,Concealed accidental hemorrhage
237275001,Mixed accidental hemorrhage
237276000,Indeterminate antepartum hemorrhage
237277009,Marginal placental hemorrhage
237284001,Symptomatic disorders in pregnancy
237285000,Gestational edema
237286004,Vulval varices in pregnancy
237288003,Abnormal weight gain in pregnancy
237292005,Placental insufficiency
237294006,Retained placenta and membranes
237298009,Maternofetal transfusion
237300009,Pregnancy with isoimmunization
237302001,Kell isoimmunization in pregnancy
237303006,Duffy isoimmunization in pregnancy
237304000,Lewis isoimmunization in pregnancy
237311001,Breech delivery
237312008,"Multiple delivery, function"
237313003,Vaginal delivery following previous cesarean section
237319004,Failure to progress in labor
237320005,Failure to progress in first stage of labor
237321009,Delayed delivery of triplet
237324001,Cervical dystocia
237325000,Head entrapment during breech delivery
237327008,Obstetric pelvic ligament damage
237328003,Obstetric pelvic joint damage
237329006,Urethra injury - obstetric
237336007,Fibrinolysis - postpartum
237337003,Afibrinogenemia - postpartum
237338008,Blood dyscrasia puerperal
237343001,Puerperal phlebitis
237348005,Puerperal pyrexia
237349002,Mild postnatal depression
237350002,Severe postnatal depression
237351003,Mild postnatal psychosis
237352005,Severe postnatal psychosis
237357004,Vascular engorgement of breast
237364002,Stillbirth
237365001,Fresh stillbirth
237366000,Macerated stillbirth
23793007,Miscarriage without complication
238613007,Generalized pustular psoriasis of pregnancy
238820002,Erythema multiforme of pregnancy
23885003,Inversion of uterus during delivery
239101008,Pregnancy eruption
239102001,Pruritus of pregnancy
239103006,Prurigo of pregnancy
239104000,Pruritic folliculitis of pregnancy
239105004,Transplacental herpes gestationis
240160002,Transient osteoporosis of hip in pregnancy
24095001,Placenta previa partialis
24146004,Premature birth of newborn quintuplets
241491007,Ultrasound scan of fetus
241493005,Ultrasound scan for fetal growth
241494004,Ultrasound scan for amniotic fluid volume
24258008,Damage to pelvic joints AND/OR ligaments during delivery
243826008,Antenatal care status
243827004,Antenatal RhD antibody status
24444009,Failed attempted abortion with sepsis
24699006,Arrested active phase of labor
247421008,Removal of secundines by aspiration curettage
248896000,Products of conception in vagina
248937008,Products of conception at uterine os cervix
248985009,Presentation of pregnancy
248996001,Transversely contracted pelvis
249013004,Pregnant abdomen finding
249014005,Finding of shape of pregnant abdomen
249017003,Pregnant uterus displaced laterally
249018008,Irritable uterus
249020006,Cervical observation during pregnancy and labor
249032005,Finding of speed of delivery
249037004,Caul membrane over baby's head at delivery
249064003,"Oblique lie, head in iliac fossa"
249065002,"Oblique lie, breech in iliac fossa"
249089009,Fetal mouth presenting
249090000,Fetal ear presenting
249091001,Fetal nose presenting
249098007,Knee presentation
249099004,Single knee presentation
249100007,Double knee presentation
249104003,Dorsoanterior shoulder presentation
249105002,Dorsoposterior shoulder presentation
249122000,Baby overdue
249142009,Slow progress in first stage of labor
249144005,Rapid first stage of labor
249145006,Uterine observation in labor
249146007,Segments of uterus distinguishable in labor
249147003,State of upper segment retraction during labor
249148008,Hypertonic lower uterine segment during labor
249149000,Contraction of uterus during labor
249150000,Transmission of uterine contraction wave
249151001,Reversal of uterine contraction wave
249161008,Maternal effort during second stage of labor
249162001,Desire to push in labor
249166003,Failure to progress in second stage of labor
249170006,Complete placenta at delivery
249172003,Finding of consistency of placenta
249173008,Placenta gritty
249174002,Placenta calcified
249175001,Placental vessel finding
249177009,Retroplacental clot
249189006,Wharton's jelly excessive
249195007,Maternal condition during labor
249196008,Distress from pain in labor
249205008,Placental fragments in uterus
249206009,Placental fragments at cervical os
249207000,Membrane at cervical os
249218000,Postpartum vulval hematoma
249219008,Genital tear resulting from childbirth
249220002,Vaginal tear resulting from childbirth
25026004,"Gestation period, 18 weeks"
25032009,Failed attempted abortion with laceration of cervix
25053000,Obstetrical central nervous system complication of anesthesia AND/OR sedation
25113000,Chorea gravidarum
25192009,"Premature birth of fraternal twins, both living"
25296001,Delivery by Scanzoni maneuver
25404008,Illegal abortion with electrolyte imbalance
25519006,Illegal abortion with perforation of vagina
25691001,Legal abortion with septic shock
25749005,Disproportion between fetus and pelvis
25825004,Hemorrhage in early pregnancy
25828002,Mid forceps delivery with episiotomy
25922000,"Major depressive disorder, single episode with postpartum onset"
26010008,Legal abortion with pelvic peritonitis
26050006,Suture of old obstetrical laceration of vagina
26158002,Uterine inertia
26224003,Failed attempted abortion complicated by genital-pelvic infection
26313002,Delivery by vacuum extraction with episiotomy
265062002,Dilation of cervix uteri and curettage of products of conception from uterus
265639000,Midforceps delivery without rotation
265640003,Crede placental expression
26623000,Failed attempted termination of pregnancy complicated by delayed and/or excessive hemorrhage
266784003,Manual replacement of inverted postnatal uterus
26690008,"Gestation period, 8 weeks"
267193004,Incomplete legal abortion
267194005,Complete legal abortion
267197003,"Antepartum hemorrhage, abruptio placentae and placenta previa"
267199000,Antepartum hemorrhage with coagulation defect
267257007,Labor and delivery complicated by fetal heart rate anomaly
267265005,Hypertonic uterine inertia
267268007,Trauma to perineum and/or vulva during delivery
267269004,Vulval and perineal hematoma during delivery
267271004,Obstetric trauma damaging pelvic joints and ligaments
267272006,Postpartum coagulation defects
267273001,Retained placenta or membranes with no hemorrhage
267276009,Obstetric anesthesia with pulmonary complications
267278005,Deliveries by vacuum extractor
267335003,Fetoplacental problems
267340006,Maternal pyrexia in labor
26741000,Abnormal amnion
26743002,Illegal abortion complicated by embolism
26828006,Rectocele affecting pregnancy
268445003,Ultrasound scan - obstetric
268475008,Outcome of delivery
268479002,Incomplete placenta at delivery
268585006,Placental infarct
268809007,Fetal or neonatal effect of malposition or disproportion during labor or delivery
268811003,Fetal or neonatal effect of abnormal uterine contractions
268812005,Fetal or neonatal effect of uterine inertia or dysfunction
268813000,Fetal or neonatal effect of destructive operation to aid delivery
268865003,Had short umbilical cord
27015006,Legal abortion with amniotic fluid embolism
270498000,Malposition and malpresentation of fetus
27068000,Failed attempted abortion with afibrinogenemia
271368004,Delivered by low forceps delivery
271369007,Delivered by mid-cavity forceps delivery
271370008,Deliveries by breech extraction
271373005,Deliveries by spontaneous breech delivery
271403007,Placenta infarcted
271442007,Fetal anatomy study
27152008,Hyperemesis gravidarum before end of 22 week gestation with carbohydrate depletion
27169005,Legal abortion complicated by shock
271954000,Failed attempted medical abortion
27215002,Uterine inversion
27388005,Partial placenta previa with intrapartum hemorrhage
273982004,Obstetric problem affecting fetus
273984003,Delivery problem for fetus
274117006,Pregnancy and infectious disease
274118001,Venereal disease in pregnancy
274119009,Rubella in pregnancy
274121004,Cardiac disease in pregnancy
274122006,Gravid uterus - retroverted
274125008,Elderly primiparous with labor
274127000,Abnormal delivery
274128005,Brow delivery
274129002,Face delivery
274130007,Emergency cesarean section
274514009,"Delivery conduct, function"
274972007,Dilatation of cervix and curettage of retained products of conception
274973002,Dilation of cervix uteri and curettage for termination of pregnancy
275168001,Neville-Barnes forceps delivery
275169009,Simpson's forceps delivery
275306006,Postnatal data
275412000,Cystitis of pregnancy
275421004,Miscarriage with heavy bleeding
275425008,Retained products after miscarriage
275426009,Pelvic disproportion
275427000,Retained membrane without hemorrhage
275429002,Delayed delivery of second twin
275434003,Stroke in the puerperium
276367008,Wanted pregnancy
276445008,Antenatal risk factors
276479009,Retained membrane
276508000,Hydrops fetalis
276509008,Non-immune hydrops fetalis
276580005,Atypical isoimmunization of newborn
276641008,Intrauterine asphyxia
276642001,Antepartum fetal asphyxia
276881003,Secondary abdominal pregnancy
27696007,True knot of umbilical cord
278056007,Uneffaced cervix
278058008,Partially effaced cervix
278094007,Rapid rate of delivery
278095008,Normal rate of delivery
278096009,Slow rate of delivery
279225001,Maternity blues
28030000,Twin birth
280732008,Obstetric disorder of uterus
281050002,Livebirth
281052005,Triplet birth
281307002,Uncertain viability of pregnancy
281687006,Face to pubes birth
282020008,Premature delivery
284075002,Spotting per vagina in pregnancy
285409006,Medical termination of pregnancy
28542003,Wigand-Martin maneuver
285434006,Insertion of abortifacient suppository
2858002,Puerperal sepsis
286996009,Blighted ovum and/or carneous mole
28701003,Low maternal weight gain
287976008,Breech/instrumental delivery operations
287977004,Dilation/incision of cervix - delivery aid
288039005,Internal cephalic version and extraction
288042004,Hysterectomy and fetus removal
288045002,Injection of amnion for termination of pregnancy
288141009,Fetal head - manual flexion
288189000,Induction of labor by intravenous injection
288190009,Induction of labor by intravenous drip
288191008,Induction of labor by injection into upper limb
288193006,Supervision - normal delivery
288194000,Routine episiotomy and repair
288209009,Normal delivery - occipitoanterior
288210004,Abnormal head presentation delivery
288261009,Was malpresentation pre-labor
288265000,Born after precipitate delivery
288266004,Born after induced labor
28860009,Prague maneuver
289203002,Finding of pattern of pregnancy
289204008,Finding of quantity of pregnancy
289205009,Finding of measures of pregnancy
289208006,Finding of viability of pregnancy
289209003,Pregnancy problem
289210008,Finding of first stage of labor
289211007,First stage of labor established
289212000,First stage of labor not established
289213005,Normal length of first stage of labor
289214004,Normal first stage of labor
289215003,First stage of labor problem
289216002,Finding of second stage of labor
289217006,Second stage of labor established
289218001,Second stage of labor not established
289219009,Rapid second stage of labor
289220003,Progressing well in second stage
289221004,Normal length of second stage of labor
289222006,Second stage of labor problem
289223001,Normal second stage of labor
289224007,Finding of delivery push in labor
289226009,Pushing effectively in labor
289227000,Not pushing well in labor
289228005,Urge to push in labor
289229002,Reluctant to push in labor
289230007,Pushing voluntarily in labor
289231006,Pushing involuntarily in labor
289232004,Finding of third stage of labor
289233009,Normal length of third stage of labor
289234003,Prolonged third stage of labor
289235002,Rapid expulsion of placenta
289236001,Delayed expulsion of placenta
289237005,Normal rate of expulsion of placenta
289238000,Finding of pattern of labor
289239008,Finding of duration of labor
289240005,Long duration of labor
289241009,Short duration of labor
289242002,Finding of blood loss in labor
289243007,Maternal blood loss minimal
289244001,Maternal blood loss within normal limits
289245000,Maternal blood loss moderate
289246004,Maternal blood loss heavy
289247008,Finding of measures of labor
289252003,Estimated maternal blood loss
289253008,Device-associated finding of labor
289254002,Expulsion of intrauterine contraceptive device during third stage of labor
289256000,Mother delivered
289257009,Mother not delivered
289258004,Finding of pattern of delivery
289259007,Vaginal delivery
289261003,Delivery problem
289263000,Large placenta
289264006,Small placenta
289265007,Placenta normal size
289267004,Finding of completeness of placenta
289269001,Lesion of placenta
289270000,Fresh retroplacental clot
289271001,Old retroplacental clot
289272008,Placenta infected
289273003,Placenta fatty deposits
289275005,Placenta offensive odor
289276006,Finding of color of placenta
289277002,Placenta pale
289278007,Finding of measures of placenta
289279004,Placenta healthy
289280001,Placenta unhealthy
289349009,Presenting part ballottable
289350009,Ballottement of fetal head abdominally
289351008,Ballottement of fetal head at fundus
289352001,Ballottement of fetal head in suprapubic area
289353006,Ballottement of fetal head vaginally
289354000,Presenting part not ballottable
289355004,Oblique lie head in right iliac fossa
289356003,Oblique lie head in left iliac fossa
289357007,Oblique lie breech in right iliac fossa
289358002,Oblique lie breech in left iliac fossa
289572007,No liquor observed vaginally
289573002,Finding of passing of operculum
289574008,Operculum passed
289575009,Operculum not passed
289606001,Maternity pad damp with liquor
289607005,Maternity pad wet with liquor
289608000,Maternity pad soaked with liquor
289675001,Finding of gravid uterus
289676000,Gravid uterus present
289677009,Gravid uterus absent
289678004,Gravid uterus not observed
289679007,Finding of size of gravid uterus
289680005,Gravid uterus large-for-dates
289681009,Gravid uterus small-for-dates
289682002,Finding of height of gravid uterus
289683007,Fundal height high for dates
289684001,Fundal height equal to dates
289685000,Fundal height low for dates
289686004,Ovoid pregnant abdomen
289687008,Rounded pregnant abdomen
289688003,Transversely enlarged pregnant abdomen
289689006,Finding of arrangement of gravid uterus
289690002,Gravid uterus central
289691003,Gravid uterus deviated to left
289692005,Gravid uterus deviated to right
289693000,Normal position of gravid uterus
289694006,Finding of sensation of gravid uterus
289699001,Finding of uterine contractions
289700000,Uterine contractions present
289701001,Uterine contractions absent
289702008,Uterine contractions ceased
289703003,Finding of pattern of uterine contractions
289704009,Finding of frequency of uterine contraction
289705005,Intermittent uterine contractions
289706006,Occasional uterine tightenings
289707002,Finding of regularity of uterine contraction
289708007,Regular uterine contractions
289709004,Finding of quantity of uterine contraction
289710009,Excessive uterine contraction
289711008,Reduced uterine contraction
289712001,Niggling uterine contractions
289714000,Finding of strength of uterine contraction
289715004,Mild uterine contractions
289716003,Moderate uterine contractions
289717007,Fair uterine contractions
289718002,Good uterine contractions
289719005,Strong uterine contractions
289720004,Very strong uterine contractions
289721000,Variable strength uterine contractions
289722007,Normal strength uterine contractions
289723002,Finding of duration of uterine contraction
289724008,Continuous contractions
289725009,Hypertonic contractions
289726005,Finding of effectiveness of uterine contraction
289727001,Effective uterine contractions
289728006,Non-effective uterine contractions
289729003,Expulsive uterine contractions
289730008,Finding of painfulness of uterine contraction
289731007,Painless uterine contractions
289732000,Painful uterine contractions
289733005,Premature uterine contraction
289734004,Delayed uterine contraction
289735003,Persistent uterine contraction
289736002,Inappropriate uterine contraction
289737006,Finding of measures of uterine contractions
289738001,Uterine contractions normal
289739009,Uterine contractions problem
289740006,Finding of contraction state of uterus
289741005,Uterus contracted
289742003,Uterus relaxed
289743008,Finding of upper segment retraction
289744002,Poor retraction of upper segment
289745001,Excessive retraction of upper segment
289746000,Normal retraction of upper segment
289747009,Finding of measures of gravid uterus
289748004,Gravid uterus normal
289749007,Gravid uterus problem
289761004,Finding of cervical dilatation
289762006,Cervix dilated
289763001,Rim of cervix palpable
289764007,Cervix undilated
289765008,Ripe cervix
289766009,Finding of thickness of cervix
289768005,Cervix thick
289769002,Cervix thinning
289770001,Cervix thin
289771002,Cervix paper thin
289827009,Finding of cervical cerclage suture
289828004,Cervical cerclage suture absent
28996007,Stillbirth of premature female (1000-2499 gms.)
290653008,Postpartum hypopituitarism
291665000,Postpartum intrapituitary hemorrhage
29171003,Trapped placenta
29397004,Repair of old obstetric urethral laceration
29399001,Elderly primigravida
29421008,Failed attempted abortion complicated by renal failure
29583006,Illegal abortion with laceration of broad ligament
29613008,Delivery by double application of forceps
29682007,Dilation and curettage for termination of pregnancy
29851005,Halo sign
29950007,Legal abortion with septic embolism
29995000,Illegal abortion with fat embolism
29997008,Premature birth of newborn triplets
300927001,Episiotomy infection
30165006,"Premature birth of fraternal twins, one living, one stillborn"
301801008,Finding of position of pregnancy
302080006,Finding of birth outcome
302253005,Delivered by cesarean section - pregnancy at term
302254004,Delivered by cesarean delivery following previous cesarean delivery
302375005,Operative termination of pregnancy
302382009,Breech extraction with internal podalic version
302383004,Forceps delivery
302384005,Controlled cord traction of placenta
302644007,Abnormal immature chorion
303063000,Eclampsia in puerperium
30476003,Barton's forceps delivery
30479005,Legal abortion with afibrinogenemia
30653008,Previous pregnancies 5
306727001,"Breech presentation, delivery, no version"
307337003,Duffy isoimmunization of the newborn
307338008,Kidd isoimmunization of the newborn
307534009,Urinary tract infection in pregnancy
307733001,Inevitable abortion complete
307734007,Complete inevitable abortion complicated by genital tract and pelvic infection
307735008,Complete inevitable abortion complicated by delayed or excessive hemorrhage
307737000,Inevitable abortion incomplete
307738005,Complete inevitable abortion without complication
307746006,Incomplete inevitable abortion complicated by embolism
307748007,Incomplete inevitable abortion without complication
307749004,Incomplete inevitable abortion complicated by genital tract and pelvic infection
307750004,Complete inevitable abortion complicated by embolism
307752007,Incomplete inevitable abortion complicated by delayed or excessive hemorrhage
307813007,Antenatal ultrasound scan at 22-40 weeks
308037008,Syntocinon induction of labor
30806007,Miscarriage with endometritis
308135003,Genital varices in the puerperium
308137006,Superficial thrombophlebitis in puerperium
308140006,Hemorrhoids in pregnancy
308187004,Vaginal varices in the puerperium
30850008,"Hemorrhage in early pregnancy, antepartum"
309469004,Spontaneous vertex delivery
310248000,Antenatal care midwifery led
31026002,Obstetrical complication of sedation
31041000119106,Obstructed labor due to deep transverse arrest and persistent occipitoposterior position
310592002,Pregnancy prolonged - 41 weeks
310594001,Pregnancy prolonged - 42 weeks
310602001,Expression of retained products of conception
310603006,Digital evacuation of retained products of conception
310604000,Suction evacuation of retained products of conception
31159001,Bluish discoloration of cervix
31207002,Self-induced abortion
31208007,Medical induction of labor
313017000,Anhydramnios
313178001,Gestation less than 24 weeks
313179009,"Gestation period, 24 weeks"
313180007,Gestation greater than 24 weeks
31383003,Tumor of vagina affecting pregnancy
314204000,Early stage of pregnancy
31481000,Vascular anomaly of umbilical cord
315307003,Repair of obstetric laceration of lower urinary tract
315308008,Dilatation of cervix for delivery
31563000,Asymptomatic bacteriuria in pregnancy
31601007,Combined pregnancy
31805001,Fetal disproportion
31821006,Uterine scar from previous surgery affecting pregnancy
31939001,Repair of obstetric laceration of cervix
31998007,"Echography, scan B-mode for fetal growth rate"
320003,"Cervical dilatation, 1cm"
3230006,Illegal abortion with afibrinogenemia
32999002,Complication of obstetrical surgical wound
33046001,Illegal abortion with urinary tract infection
33340004,Multiple conception
33348006,Previous pregnancies 7
33370009,Hyperemesis gravidarum before end of 22 week gestation with electrolyte imbalance
33490001,Failed attempted abortion with fat embolism
33552005,Anomaly of placenta
33561005,Illegal abortion with acute renal failure
33627001,Prolonged first stage of labor
33654003,Repair of old obstetric laceration of urinary bladder
33807004,Internal and combined version with extraction
34089005,Premature birth of newborn quadruplets
34100008,"Premature birth of identical twins, both living"
34262005,Fourth degree perineal laceration involving rectal mucosa
34270000,Miscarriage complicated by shock
34327003,Parturient hemorrhage associated with hyperfibrinolysis
34367002,Failed attempted abortion with perforation of bowel
34478009,Failed attempted abortion with defibrination syndrome
34500003,Legal abortion without complication
34530006,Failed attempted abortion with electrolyte imbalance
34614007,Miscarriage with complication
34701001,Illegal abortion with postoperative shock
34801009,Ectopic pregnancy
34842007,Antepartum hemorrhage
34981006,Hypertonic uterine dysfunction
35347003,Delayed delivery after artificial rupture of membranes
35381000119101,Quadruplet pregnancy with loss of one or more fetuses
35537000,Ladin's sign
35574001,"Delivery of shoulder presentation, function"
35608009,Stillbirth of immature male (500-999 gms.)
35656003,Intraligamentous pregnancy
35716006,Cracked nipple associated with childbirth
35746009,Uterine fibroids affecting pregnancy
35874009,Normal labor
35882009,Abnormality of forces of labor
359937006,Repair of obstetric laceration of anus
359940006,Partial breech extraction
359943008,Partial breech delivery
359946000,Repair of current obstetric laceration of rectum and sphincter ani
35999006,Blighted ovum
361095003,Dehiscence AND/OR disruption of uterine wound in the puerperium
361096002,Disruption of cesarean wound in the puerperium
36144008,McClintock's sign
36248000,Repair of obstetric laceration of urethra
36297009,Septate vagina affecting pregnancy
362972006,Disorder of labor / delivery
362973001,Disorder of puerperium
3634007,Legal abortion complicated by metabolic disorder
363681007,Pregnancy with abortive outcome
36428009,"Gestation period, 42 weeks"
364587008,Birth outcome
36473002,Legal abortion with oliguria
364738009,Finding of outcome of delivery
36497006,Traumatic extension of episiotomy
366323009,Finding of length of gestation
366325002,Finding of progress of labor - first stage
366327005,Finding of progress of second stage of labor
366328000,Finding related to ability to push in labor
366329008,Finding of speed of delivery of placenta
366330003,Finding of size of placenta
366332006,Finding of odor of placenta
36664002,Failed attempted abortion with air embolism
36697001,False knot of umbilical cord
367494004,Premature birth of newborn
36801000119105,Continuing triplet pregnancy after spontaneous abortion of one or more fetuses
36813001,Placenta previa
36854009,Inlet contraction of pelvis
37005007,"Gestation period, 5 weeks"
370352001,Antenatal screening finding
371091008,Postpartum infection of vulva
371106008,Idiopathic maternal thrombocytopenia
371374003,Retained products of conception
371375002,Retained secundines
371380006,Amniotic fluid leaking
371614003,Hematoma of obstetric wound
372043009,Hydrops of allantois
372456005,Repair of obstetric laceration
373663005,Perinatal period
373896005,Miscarriage complicated by genital-pelvic infection
373901007,Legal abortion complicated by genital-pelvic infection
373902000,Illegal abortion complicated by genital-pelvic infection
37762002,Face OR brow presentation of fetus
37787009,Legal abortion with blood-clot embolism
3798002,"Premature birth of identical twins, both stillborn"
38010008,Intrapartum hemorrhage
38039008,"Gestation period, 10 weeks"
38099005,Failed attempted abortion with endometritis
382341000000101,Total number of registerable births at delivery
38250004,Arrested labor
38257001,"Term birth of identical twins, one living, one stillborn"
38451000119105,Failed attempted vaginal birth after previous cesarean section
384729004,Delivery of vertex presentation
384730009,Delivery of cephalic presentation
38479009,Frank breech delivery
38534008,Aspiration of stomach contents after anesthesia AND/OR sedation in labor AND/OR delivery
386235000,Childbirth preparation
386322007,High risk pregnancy care
386343008,Labor suppression
386639001,Termination of pregnancy
386641000,Therapeutic abortion procedure
38720006,Septuplet pregnancy
387692004,Hypotonic uterine inertia
387696001,Primary hypotonic uterine dysfunction
387699008,Primary uterine inertia
387700009,Prolonged latent phase of labor
387709005,Removal of ectopic fetus from abdominal cavity
387710000,Removal of extrauterine ectopic fetus
387711001,Pubiotomy to assist delivery
38784004,Illegal abortion complicated by damage to pelvic organs AND/OR tissues
3885002,ABO isoimmunization affecting pregnancy
38905006,Illegal abortion with amniotic fluid embolism
38951007,Failed attempted abortion with laceration of bowel
39101000119109,Pregnancy with isoimmunization from irregular blood group incompatibility
391131000119106,Ultrasonography for qualitative amniotic fluid volume
39120007,Failed attempted abortion with salpingo-oophoritis
39121000119100,Pelvic mass in pregnancy
391896006,Therapeutic abortion by aspiration curettage
391897002,Aspiration curettage of uterus for termination of pregnancy
39199000,Miscarriage with laceration of bladder
391997001,Operation for retained placenta
391998006,Dilation and curettage of uterus after delivery
392000009,Hysterotomy for retained placenta
39208009,Fetal hydrops causing disproportion
39213008,"Premature birth of identical twins, one living, one stillborn"
39239006,Legal abortion with electrolyte imbalance
39246002,Legal abortion with sepsis
39249009,Placentitis
39333005,"Cervical dilatation, 8cm"
39406005,Legally induced abortion
394211000119109,Ultrasonography for fetal biophysical profile with non-stress testing
394221000119102,Ultrasonography for fetal biophysical profile without non-stress testing
3944006,Placental sulfatase deficiency (X-linked steryl-sulfatase deficiency) in a female
3950001,Birth
396544001,Cesarean wound disruption
397752008,Obstetric perineal wound disruption
397949005,Poor fetal growth affecting management
397952002,Trauma to perineum during delivery
398019008,Perineal laceration during delivery
39804004,Abnormal products of conception
398262004,Disruption of episiotomy wound in the puerperium
398307005,Low cervical cesarean section
399031001,Fourth degree perineal laceration
399363000,Late fetal death affecting management of mother
400170001,Hypocalcemia of puerperium
4006006,Fetal tachycardia affecting management of mother
40219000,Delivery by Malstrom's extraction with episiotomy
4026005,Interstitial mastitis associated with childbirth
402836009,Spider telangiectasis in association with pregnancy
403528000,Pregnancy-related exacerbation of dermatosis
40405003,Illegal abortion with cerebral anoxia
40444006,Miscarriage with acute necrosis of liver
40500006,Illegal abortion with laceration of uterus
40521000119100,Postpartum pregnancy-induced hypertension
405256006,Parturient paresis
405733001,Hypocalcemia of late pregnancy or lactation
405736009,Accidental antepartum hemorrhage
40704000,Wright's obstetrical version with extraction
40791000119105,Postpartum gestational diabetes mellitus
40792007,Kristeller maneuver
40801000119106,Gestational diabetes mellitus complicating pregnancy
408783007,Antenatal Anti-D prophylaxis status
408796008,Stillbirth - unknown if fetal death intrapartum or prior to labor
408814002,Ultrasound scan for fetal anomaly
408815001,Ultrasound scan for fetal nuchal translucency
408818004,Induction of labor by artificial rupture of membranes
408819007,Delivery of placenta by maternal effort
408823004,Antenatal hepatitis B blood screening test status
408825006,Antenatal hepatitis B blood screening test sent
408827003,Antenatal human immunodeficiency virus blood screening test status
408828008,Antenatal human immunodeficiency virus blood screening test sent
408831009,Antenatal thalassemia blood screening test status
408833007,Antenatal thalassemia blood screening test sent
408840008,Extra-amniotic termination of pregnancy
408842000,Antenatal human immunodeficiency virus blood screening test requested
408843005,Antenatal thalassemia blood screening test requested
408883002,Breastfeeding support
41059002,Cesarean hysterectomy
41215002,"Congenital abnormality of uterus, affecting pregnancy"
413338003,Incomplete miscarriage with complication
413339006,Failed trial of labor - delivered
41438001,"Gestation period, 21 weeks"
414880004,Nuchal ultrasound scan
415105001,Placental abruption
41587001,Third trimester pregnancy
416055001,Total breech extraction
416402001,Gestational trophoblastic disease
416413003,Advanced maternal age gravida
416669000,Invasive hydatidiform mole
417006004,Twin reversal arterial perfusion syndrome
417044008,"Hydatidiform mole, benign"
417121007,Breech extraction
417150000,Exaggerated placental site
417364008,Placental site nodule
417570003,Gestational choriocarcinoma
41806003,Legal abortion with laceration of broad ligament
418090003,Ultrasound obstetric doppler
42102002,"Pre-admission observation, undelivered mother"
42170009,Abnormal amniotic fluid
422808006,Prenatal continuous visit
423445003,Difficulty following prenatal diet
423834007,Difficulty with prenatal rest pattern
42390009,Repair of obstetric laceration of bladder and urethra
424037008,Difficulty following prenatal exercise routine
424441002,Prenatal initial visit
424525001,Antenatal care
424619006,Prenatal visit
42537006,Secondary perineal tear in the puerperium
42553009,Deficiency of placental barrier function
425551008,Antenatal ultrasound scan for possible abnormality
425708006,Placental aromatase deficiency
42571002,Failed induction of labor
42599006,Trauma from instrument during delivery
426295007,Obstetric uterine artery Doppler
426403007,Late entry into prenatal care
426840007,Fetal biometry using ultrasound
42686001,Chromosomal abnormality in fetus affecting obstetrical care
426997005,Traumatic injury during pregnancy
427013000,Alcohol consumption during pregnancy
427139004,Third trimester bleeding
427623005,Obstetric umbilical artery Doppler
42783002,"Spontaneous placental expulsion, Schultz mechanism"
428017002,Condyloma acuminata of vulva in pregnancy
428058009,Gestation less than 9 weeks
428164004,Mitral valve disorder in pregnancy
428230005,Trichomonal vaginitis in pregnancy
428252001,Vaginitis in pregnancy
428508008,Abortion due to Brucella abortus
428511009,Multiple pregnancy with one fetal loss
428566005,Gestation less than 20 weeks
428567001,Gestation 14 - 20 weeks
42857002,Second stage of labor
428930004,Gestation 9- 13 weeks
429187001,Continuing pregnancy after intrauterine death of twin fetus
429240000,Third trimester pregnancy less than 36 weeks
429715006,Gestation greater than 20 weeks
430063002,Transvaginal nuchal ultrasonography
430064008,Transvaginal obstetric ultrasonography
430881000,Second trimester bleeding
430933008,Gravid uterus size for dates discrepancy
431868002,Initiation of breastfeeding
432246004,Transvaginal obstetric doppler ultrasonography
43293004,Failed attempted abortion with septic embolism
43306002,Miscarriage complicated by embolism
433153009,Chorionic villus sampling using obstetric ultrasound guidance
43651009,Acquired stenosis of vagina affecting pregnancy
43673002,Fetal souffle
43697006,"Gestation period, 37 weeks"
43715006,Secondary uterine inertia
43808007,"Cervical dilatation, 4cm"
439311009,Intends to continue pregnancy
43970002,Congenital stenosis of vagina affecting pregnancy
43990006,Sextuplet pregnancy
44101005,Illegal abortion with air embolism
441619002,Repair of obstetric laceration of perineum and anal sphincter and mucosa of rectum
441697004,Thrombophilia associated with pregnancy
441924001,Gestational age unknown
44216000,"Retained products of conception, following delivery with hemorrhage"
442478007,Multiple pregnancy involving intrauterine pregnancy and tubal pregnancy
443006,Cystocele affecting pregnancy
443460007,Multigravida of advanced maternal age
44398003,"Gestation period, 4 weeks"
444661007,High risk pregnancy due to history of preterm labor
445086005,Chemical pregnancy
4451004,Illegal abortion with renal tubular necrosis
445149007,Residual trophoblastic disease
445548006,Dead fetus in utero
445866007,Ultrasonography of multiple pregnancy for fetal anomaly
445912000,Excision of fallopian tube and surgical removal of ectopic pregnancy
446208007,Ultrasonography in second trimester
446353007,Ultrasonography in third trimester
44640004,Failed attempted abortion with parametritis
446522006,Ultrasonography in first trimester
446810002,Ultrasonography of multiple pregnancy for fetal nuchal translucency
446920006,Transvaginal ultrasonography to determine the estimated date of confinement
44772007,Maternal obesity syndrome
44782008,Molar pregnancy
44795003,Rhesus isoimmunization affecting pregnancy
447972007,Medical termination of pregnancy using prostaglandin
44814008,Miscarriage with laceration of periurethral tissue
44979007,Carneous mole
449807005,Type 3a third degree laceration of perineum
449808000,Type 3b third degree laceration of perineum
449809008,Type 3c third degree laceration of perineum
44992005,Failed attempted abortion with intravascular hemolysis
4504004,Potter's obstetrical version with extraction
450483001,Cesarean section through inverted T shaped incision of uterus
450484007,Cesarean section through J shaped incision of uterus
450640001,Open removal of products of conception from uterus
450678004,Intraamniotic injection of abortifacient
450679007,Extraamniotic injection of abortifacient
450798003,Delivery of occipitoposterior presentation
451014004,Curettage of products of conception from uterus
45139008,"Gestation period, 29 weeks"
45307008,Extrachorial pregnancy
45384004,Multiple birth
45718005,Vaginal delivery with forceps including postpartum care
45757002,Labor problem
45759004,Rigid perineum affecting pregnancy
4576001,Legal abortion complicated by renal failure
459166009,Dichorionic diamniotic twin pregnancy
459167000,Monochorionic twin pregnancy
459168005,Monochorionic diamniotic twin pregnancy
459169002,Monochorionic diamniotic twin pregnancy with similar amniotic fluid volumes
459170001,Monochorionic diamniotic twin pregnancy with dissimilar amniotic fluid volumes
459171002,Monochorionic monoamniotic twin pregnancy
46022004,Bearing down reflex
46230007,"Gestation period, 40 weeks"
46273003,Abscess of nipple associated with childbirth
46311005,Perineal laceration involving fourchette
46365005,Failed attempted abortion with perforation of periurethral tissue
46502006,Hematoma of vagina during delivery
46894009,"Gestational diabetes mellitus, class A>2<"
46906003,"Gestation period, 27 weeks"
47161002,Failed attempted abortion with perforation of cervix
47200007,High risk pregnancy
472321009,Continuing pregnancy after intrauterine death of one twin with intrauterine retention of dead twin
47236005,Third stage hemorrhage
47267007,Fetal or neonatal effect of destructive operation on live fetus to facilitate delivery
472839005,Termination of pregnancy after first trimester
47537002,Miscarriage with postoperative shock
4787007,Fetal or neonatal effect of breech delivery and extraction
480571000119102,Doppler ultrasound velocimetry of umbilical artery of fetus
48204000,"Spontaneous unassisted delivery, medical personnel present"
48433002,Legal abortion with complication
48485000,Miscarriage in third trimester
48688005,"Gestation period, 26 weeks"
48739004,Miscarriage with cardiac arrest and/or cardiac failure
48775002,Repair of obstetric laceration of pelvic floor
48782003,Delivery normal
4886009,Premature birth of newborn male
48888007,Placenta previa found before labor AND delivery by cesarean section without hemorrhage
48975005,Stimulated labor
4907004,Non-involution of uterus
49177006,Postpartum coagulation defect with hemorrhage
49279000,Slow slope active phase of labor
49342001,Stricture of vagina affecting pregnancy
49364005,Subareolar abscess associated with childbirth
49416000,Failed attempted abortion
49550006,Premature pregnancy delivered
49561003,Rupture of gravid uterus before onset of labor
49632008,Illegally induced abortion
49815007,Damage to coccyx during delivery
49964003,Ectopic fetus
50258003,Fetal or neonatal effect of hypotonic uterine dysfunction
50367001,"Gestation period, 11 weeks"
50557007,Healed pelvic floor repair affecting pregnancy
50726009,Failed attempted abortion with perforation of uterus
50758004,Term birth of newborn twins
50770000,Miscarriage with defibrination syndrome
50844007,Failed attempted abortion with pelvic peritonitis
51096002,Legal abortion with pulmonary embolism
51154004,Obstetrical pulmonary complication of anesthesia AND/OR sedation
51195001,Placental polyp
51495008,Cerebral anoxia following anesthesia AND/OR sedation in labor AND/OR delivery
51519001,Marginal insertion of umbilical cord
51707007,Legal abortion with salpingo-oophoritis
51885006,Morning sickness
51920004,Precipitate labor
51953009,Legal abortion with perforation of broad ligament
51954003,Miscarriage with perforation of periurethral tissue
521000119104,"Acute cystitis in pregnancy, antepartum"
522101000000109,Fetal death before 24 weeks with retention of dead fetus
5231000179108,Three dimensional obstetric ultrasonography
52327008,Fetal myelomeningocele causing disproportion
52342006,Legal abortion with acute renal failure
5241000179100,Three dimensional obstetric ultrasonography in third trimester
52483005,Term birth of newborn sextuplets
52588004,Robert's sign
52660002,Induced abortion following intra-amniotic injection with hysterotomy
52772002,Postpartum thyroiditis
52942000,Term birth of stillborn twins
53024001,Insufficient weight gain of pregnancy
53098006,Legal abortion with salpingitis
53111003,Failed attempted abortion with postoperative shock
53183006,Miscarriage with intravascular hemolysis
53212003,Postobstetric urethral stricture
53247006,Illegal abortion with laceration of bladder
53443007,Prolonged labor
53638009,Therapeutic abortion
53881005,Gravida 0
54044001,Legal abortion with laceration of vagina
54155004,Illegal abortion with uremia
54212005,Irregular uterine contractions
54213000,Oligohydramnios without rupture of membranes
54318006,"Gestation period, 19 weeks"
54449002,Forced uterine inversion
54529009,Illegal abortion with cardiac arrest AND/OR failure
54559004,Uterine souffle
54650005,Premature birth of stillborn twins
54812001,"Delivery of brow presentation, function"
54844002,Prolapse of gravid uterus
54973000,Total breech delivery with forceps to aftercoming head
55052008,Diagnostic ultrasound of gravid uterus
55187005,Tarnier's sign
55466006,Missed labor
55472006,Submammary abscess associated with childbirth
55543007,Previous pregnancies 3
5556001,Manually assisted spontaneous delivery
55581002,Meconium in amniotic fluid first noted during labor AND/OR delivery in liveborn infant
55589000,Illegal abortion with pulmonary embolism
55613002,Engorgement of breasts associated with childbirth
55639004,Failed attempted abortion with laceration of vagina
55669006,Repair of obstetrical laceration of perineum
55704005,"Abscess of breast, associated with childbirth"
5577003,Legal abortion with laceration of bowel
55933000,Legal abortion with perforation of bowel
55976003,Miscarriage with blood-clot embolism
56160003,Hydrorrhea gravidarum
56272000,Postpartum deep phlebothrombosis
56425003,Placenta edematous
56451001,Failed attempted abortion with perforation of broad ligament
56462001,Failed attempted abortion with soap embolism
56620000,Delivery of placenta following delivery of infant outside of hospital
57271003,Extraperitoneal cesarean section
57296000,Incarcerated gravid uterus
5740008,Pelvic hematoma during delivery
57411006,Colpoperineorrhaphy following delivery
57420002,Listeria abortion
57469000,Miscarriage with acute renal failure
57576007,Retroverted gravid uterus
57630001,First trimester pregnancy
57734001,Legal abortion complicated by embolism
57759005,First degree perineal laceration
57797005,Induced termination of pregnancy
57907009,"Gestation period, 36 weeks"
58123006,Failed attempted abortion with pulmonary embolism
58289000,Prodromal stage labor
58532003,Unwanted pregnancy
58699001,"Cervical dilatation, 2cm"
58703003,Postpartum depression
58705005,Bracht maneuver
58881007,Polyp of cervix affecting pregnancy
58990004,Miscarriage complicated by damage to pelvic organs and/or tissues
59204004,Miscarriage with septic embolism
59363009,Inevitable abortion
5939002,Failed attempted abortion complicated by metabolic disorder
59403008,Premature birth of newborn female
5945005,Legal abortion with urinary tract infection
59466002,Second trimester pregnancy
59566000,Oligohydramnios
59795007,Short cord
5984000,"Fetal or neonatal effect of malpresentation, malposition and/or disproportion during labor and/or delivery"
59859005,Legal abortion with laceration of bladder
59919008,Failed attempted abortion with complication
60000008,Mesometric pregnancy
60265009,Miscarriage with air embolism
60328005,Previous pregnancies 4
60401000119104,Postpartum psychosis in remission
60755004,Persistent hymen affecting pregnancy
60810003,Quadruplet pregnancy
609133009,Short cervical length in pregnancy
609204004,Subchorionic hematoma
609441001,Fetal or neonatal effect of breech delivery
609442008,Antenatal care for woman with history of recurrent miscarriage
609443003,Retained products of conception following induced termination of pregnancy
609446006,Induced termination of pregnancy with complication
609447002,Induced termination of pregnancy complicated by damage to pelvic organs and/or tissues
609449004,Induced termination of pregnancy complicated by embolism
609450004,Induced termination of pregnancy complicated by genital-pelvic infection
609451000,Induced termination of pregnancy complicated by metabolic disorder
609452007,Induced termination of pregnancy complicated by renal failure
609453002,Induced termination of pregnancy complicated by shock
609454008,Induced termination of pregnancy complicated by acute necrosis of liver
609455009,Induced termination of pregnancy complicated by acute renal failure
609456005,Induced termination of pregnancy complicated by afibrinogenemia
609457001,Induced termination of pregnancy complicated by air embolism
609458006,Induced termination of pregnancy complicated by amniotic fluid embolism
609459003,Induced termination of pregnancy complicated by blood-clot embolism
609460008,Induced termination of pregnancy complicated by cardiac arrest and/or failure
609461007,Induced termination of pregnancy complicated by cerebral anoxia
609462000,Induced termination of pregnancy complicated by defibrination syndrome
609463005,Induced termination of pregnancy complicated by electrolyte imbalance
609464004,Induced termination of pregnancy complicated by endometritis
609465003,Induced termination of pregnancy complicated by fat embolism
609466002,Induced termination of pregnancy complicated by intravascular hemolysis
609467006,Induced termination of pregnancy complicated by laceration of bowel
609468001,Induced termination of pregnancy complicated by laceration of broad ligament
609469009,Induced termination of pregnancy complicated by laceration of cervix
609470005,Induced termination of pregnancy complicated by laceration of uterus
609471009,Induced termination of pregnancy complicated by laceration of vagina
609472002,Induced termination of pregnancy complicated by acute renal failure with oliguria
609473007,Induced termination of pregnancy complicated by parametritis
609474001,Induced termination of pregnancy complicated by pelvic peritonitis
609475000,Induced termination of pregnancy complicated by perforation of bowel
609476004,Induced termination of pregnancy complicated by perforation of cervix
609477008,Induced termination of pregnancy complicated by perforation of uterus
609478003,Induced termination of pregnancy complicated by perforation of vagina
609479006,Induced termination of pregnancy complicated by postoperative shock
609480009,Induced termination of pregnancy complicated by pulmonary embolism
609482001,Induced termination of pregnancy complicated by renal tubular necrosis
609483006,Induced termination of pregnancy complicated by salpingitis
609484000,Induced termination of pregnancy complicated by salpingo-oophoritis
609485004,Induced termination of pregnancy complicated by sepsis
609486003,Induced termination of pregnancy complicated by septic embolism
609487007,Induced termination of pregnancy complicated by septic shock
609489005,Induced termination of pregnancy complicated by soap embolism
609490001,Induced termination of pregnancy complicated by uremia
609491002,Induced termination of pregnancy complicated by urinary tract infection
609492009,Induced termination of pregnancy without complication
609493004,Induced termination of pregnancy complicated by tetanus
609494005,Induced termination of pregnancy complicated by pelvic disorder
609496007,Complication occurring during pregnancy
609497003,Venous complication in the puerperium
609498008,Induced termination of pregnancy complicated by laceration of bladder
609499000,Induced termination of pregnancy complicated by laceration of periurethral tissue
609500009,Induced termination of pregnancy complicated by perforation of bladder
609501008,Induced termination of pregnancy complicated by perforation of broad ligament
609502001,Induced termination of pregnancy complicated by perforation of periurethral tissue
609503006,Induced termination of pregnancy complicated by bladder damage
609504000,Induced termination of pregnancy complicated by bowel damage
609505004,Induced termination of pregnancy complicated by broad ligament damage
609506003,Induced termination of pregnancy complicated by cardiac arrest
609507007,Induced termination of pregnancy complicated by cardiac failure
609508002,Induced termination of pregnancy complicated by cervix damage
609510000,Induced termination of pregnancy complicated by infectious disease
609511001,Induced termination of pregnancy complicated by periurethral tissue damage
609512008,Induced termination of pregnancy complicated by oliguria
609513003,Induced termination of pregnancy complicated by uterus damage
609514009,Induced termination of pregnancy complicated by vaginal damage
609515005,Epithelioid trophoblastic tumor
609516006,Gestational trophoblastic lesion
609519004,Gestational trophoblastic neoplasia
609525000,Miscarriage of tubal ectopic pregnancy
61007003,Separation of symphysis pubis during delivery
6134000,Illegal abortion with oliguria
61353001,Repair of obstetric laceration of bladder
61452007,Failed attempted abortion with laceration of uterus
61568004,Miscarriage with salpingo-oophoritis
61586001,Delivery by vacuum extraction
61714007,Metabolic disturbance in labor AND/OR delivery
61752008,Illegal abortion complicated by shock
61810006,Illegal abortion with defibrination syndrome
61881000,Osiander's sign
61893009,Laparoscopic treatment of ectopic pregnancy with oophorectomy
61951009,Failed attempted abortion with laceration of periurethral tissue
62129004,Contraction ring dystocia
62131008,Couvelaire uterus
62333002,"Gestation period, 13 weeks"
6234006,Second degree perineal laceration
62377009,Postpartum cardiomyopathy
62410004,Postpartum fibrinolysis with hemorrhage
62472004,Cervix fully dilated
62508004,Mid forceps delivery
6251000119101,Induced termination of pregnancy in first trimester
62531004,Placenta previa marginalis
62583006,Puerperal phlegmasia alba dolens
6261000119104,Induced termination of pregnancy in second trimester
62612003,Fibrosis of perineum affecting pregnancy
62657007,Cardiac arrest AND/OR failure following anesthesia AND/OR sedation in labor AND/OR delivery
62774004,"Cervical dilatation, 9cm"
62888008,Legal abortion with perforation of bladder
63110000,"Gestation period, 7 weeks"
63407004,Episioproctotomy
63503002,"Gestation period, 41 weeks"
63596003,Laparoscopic treatment of ectopic pregnancy with salpingectomy
63637002,Failed attempted abortion with laceration of broad ligament
63662002,Purulent mastitis associated with childbirth
63750008,Oblique lie
6383007,Premature labor
64171002,Obstetrical cardiac complication of anesthesia AND/OR sedation
64181003,Failed attempted abortion with perforation of vagina
64229006,Traumatic lesion during delivery
64254006,Triplet pregnancy
6473009,Suture of old obstetrical laceration of uterus
64814003,Miscarriage with electrolyte imbalance
64920003,"Gestation period, 31 weeks"
64954002,Avulsion of inner symphyseal cartilage during delivery
65035007,"Gestation period, 22 weeks"
65147003,Twin pregnancy
65243006,Delivery by midwife
65377004,Polygalactia
65409004,Obstetrical complication of anesthesia
65539006,Impetigo herpetiformis
65683006,"Gestation period, 17 weeks"
65727000,Intrauterine pregnancy
6594005,Cerebrovascular disorder in the puerperium
66119008,Disruption of perineal laceration repair in the puerperium
66131005,Miscarriage with fat embolism
66231000,"Fetal OR intrauterine asphyxia, not clear if noted before OR after onset of labor in liveborn infant"
66294006,Obstruction by abnormal pelvic soft tissues
6647006,Legal abortion with defibrination syndrome
6678005,"Gestation period, 15 weeks"
66892003,Failed attempted abortion with urinary tract infection
66895001,"Cervical dilatation, 6cm"
66958002,"Isoimmunization from non-ABO, non-Rh blood-group incompatibility affecting pregnancy"
67042008,Failed attempted abortion complicated by shock
67229002,Spontaneous uterine inversion
67465009,Miscarriage with sepsis
67486009,Postpartum pelvic thrombophlebitis
67802002,"Malpresentation other than breech, successfully converted to cephalic presentation"
6802007,Illegal abortion with perforation of broad ligament
68189005,Illegal abortion with perforation of periurethral tissue
68214002,Lymphangitis of breast associated with childbirth
6825008,Perineal laceration involving rectovaginal septum
68509000,Stillbirth of immature female (500-999 gms.)
68635007,Deficiency of placental function
6891008,"Uterine incoordination, second degree"
6893006,First stage of labor
69162008,Cleidotomy
69217004,Outlet contraction of pelvis
69270005,Rupture of uterus during AND/OR after labor
69302000,Abortion on demand
69338007,Kanter's sign
69422002,Trial forceps delivery
69777007,Interlocked twins
69802008,Cervical cerclage suture present
698414000,Fetal or neonatal effect of complication of labor
698415004,Fetal or neonatal effect of complication of delivery
698497008,Fetal or neonatal effect of disproportion during labor
698498003,Fetal or neonatal effect of disproportion during delivery
698554000,Fetal or neonatal effect of malposition during labor
698555004,Fetal or neonatal effect of malposition during delivery
698586005,Complete legal abortion complicated by excessive hemorrhage
698587001,Complete illegal abortion complicated by excessive hemorrhage
698632006,Pregnancy induced edema
698636009,Complete legal abortion with complication
698702007,Deep transverse arrest with persistent occipitoposterior position
698708006,Antepartum hemorrhage due to placenta previa type I
698709003,Antepartum hemorrhage due to placenta previa type II
698710008,Antepartum hemorrhage due to placenta previa type III
698711007,Antepartum hemorrhage due to placenta previa type IV
698712000,Antepartum hemorrhage due to cervical polyp
698713005,Antepartum haemorrhage due to cervical erosion
698716002,Preterm spontaneous labor with preterm delivery
698717006,Preterm spontaneous labor with term delivery
698791008,Fetal or neonatal effect of malpresentation during labor
698795004,Fetal or neonatal effect of malpresentation during delivery
699240001,Combined intrauterine and ovarian pregnancy
699949009,Retained placenta due to morbidly adherent placenta
699950009,Anti-D isoimmunization affecting pregnancy
699999008,Obstetrical version with extraction
700000006,Vaginal delivery of fetus
700038005,Mastitis associated with lactation
700041001,Induced termination of pregnancy under unsafe conditions
700442004,Ultrasonography of fetal ductus venosus
70068004,Persistent occipitoposterior position
70112005,Stillbirth of mature female (2500 gms. or more)
70137000,Deficiency of placental endocrine function
702452007,Quadruplet birth
702453002,Quintuplet birth
702454008,Sextuplet birth
702736005,Supervision of high risk pregnancy with history of previous cesarean section
702737001,Supervision of high risk pregnancy with history of gestational diabetes mellitus
702738006,Supervision of high risk pregnancy
702739003,Supervision of high risk pregnancy with history of previous molar pregnancy
702740001,Supervision of high risk pregnancy with history of previous precipitate labor
702741002,Supervision of high risk pregnancy for multigravida
702742009,Supervision of high risk pregnancy for social problem
702743004,Supervision of high risk pregnancy for multigravida age 15 years or younger
702744005,Supervision of high risk pregnancy for primigravida age 15 years or younger
702985005,Ultrasonography of fetal shunt
70425008,Piskacek's sign
70537007,Hegar's sign
70651004,Calkin's sign
707089008,Genital tract infection in puerperium
707207004,Incomplete induced termination of pregnancy
707254000,Amniotic adhesion
70823006,Illegal abortion with intravascular hemolysis
709004006,Emergency lower segment cesarean section with inverted T incision
70964000,Postparturient hemoglobinuria
710165007,Ultrasonography of fetal head
71028008,Fetal-maternal hemorrhage
710911000000102,Infant feeding antenatal checklist completed
71096001,Inversion of uterine contraction
71166009,Forceps delivery with rotation of fetal head
71216006,Legal abortion with laceration of periurethral tissue
712653003,Delivery by cesarean section for footling breech presentation
712654009,Delivery by cesarean section for breech presentation
712655005,Delivery by cesarean section for flexed breech presentation
713187004,Polyhydramnios due to maternal disease
713191009,Polyhydramnios due to placental anomaly
713192002,Oligohydramnios due to rupture of membranes
713202001,Antepartum stillbirth
713232009,Prolonged second stage of labor due to poor maternal effort
713233004,Supervision of high risk pregnancy with history of previous neonatal death
713234005,Supervision of high risk pregnancy with history of previous intrauterine death
713235006,Supervision of high risk pregnancy with history of previous antepartum hemorrhage
713237003,Supervision of high risk pregnancy with history of previous big baby
713238008,Supervision of high risk pregnancy with history of previous abnormal baby
713239000,Supervision of high risk pregnancy with history of previous primary postpartum hemorrhage
713240003,Supervision of high risk pregnancy with poor obstetric history
713241004,Supervision of high risk pregnancy with history of previous fetal distress
713242006,Supervision of high risk pregnancy with poor reproductive history
713249002,Pyogenic granuloma of gingiva co-occurrent and due to pregnancy
713386003,Supervision of high risk pregnancy for maternal short stature
713387007,Supervision of high risk pregnancy with family history of diabetes mellitus
71355009,"Gestation period, 30 weeks"
713575004,Dizygotic twin pregnancy
713576003,Monozygotic twin pregnancy
71362000,Illegal abortion with septic embolism
714812005,Induced termination of pregnancy
715880002,Obstructed labor due to fetal abnormality
71612002,Postpartum uterine hypertrophy
716379000,Acute fatty liver of pregnancy
71639005,Galactorrhea associated with childbirth
7166002,Legal abortion with laceration of cervix
717794008,Supervision of pregnancy with history of infertility
717795009,Supervision of pregnancy with history of insufficient antenatal care
717797001,Antenatal care of elderly primigravida
717809003,Immediate postpartum care
717810008,Routine postpartum follow-up
717816002,Infection of nipple associated with childbirth with attachment difficulty
717817006,Abscess of breast associated with childbirth with attachment difficulty
717818001,Nonpurulent mastitis associated with childbirth with attachment difficulty
717819009,Retracted nipple associated with childbirth with attachment difficulty
717820003,Cracked nipple associated with childbirth with attachment difficulty
717959008,Cardiac complication of anesthesia during the puerperium
717960003,Central nervous system complication of anesthesia during the puerperium
718475004,Ultrasonography for amniotic fluid index
71848002,Bolt's sign
71901000,Congenital contracted pelvis
72014004,Abnormal fetal duplication
72059007,Destructive procedure on fetus to facilitate delivery
721022000,Complication of anesthesia during the puerperium
721177006,Injury complicating pregnancy
72161000119100,Antiphospholipid syndrome in pregnancy
722570003,Fetal or neonatal effect of meconium passage during delivery
723541004,Disease of respiratory system complicating pregnancy
723665008,Vaginal bleeding complicating early pregnancy
72417002,Tumultuous uterine contraction
724483001,Concern about body image related to pregnancy
724484007,Incomplete legal abortion without complication
724486009,Venous disorder co-occurrent with pregnancy
724488005,Preterm delivery following induction of labor
724489002,Preterm delivery following Cesarean section
724490006,Intrapartum hemorrhage co-occurrent and due to obstructed labor with uterine rupture
724496000,Postpartum hemorrhage co-occurrent and due to uterine rupture following obstructed labor
7245003,Fetal dystocia
72492007,Footling breech delivery
72543004,"Stillbirth of immature fetus, sex undetermined (500-999 gms.)"
72544005,"Gestation period, 25 weeks"
72613009,Miscarriage with oliguria
7266006,Total placenta previa with intrapartum hemorrhage
72846000,"Gestation period, 14 weeks"
72860003,Disorder of amniotic cavity AND/OR membrane
72892002,Normal pregnancy
73161006,Transverse lie
73280003,Illegal abortion with laceration of vagina
733142005,Sepsis following obstructed labor
73341009,Removal of ectopic fetus from ovary without oophorectomy
733839001,Postpartum acute renal failure
734275002,Delivery by outlet vacuum extraction
734276001,Delivery by mid-vacuum extraction
735492001,Obstructed labor due to shoulder dystocia
736018001,Elective upper segment cesarean section with bilateral tubal ligation
736020003,Emergency upper segment cesarean section with bilateral tubal ligation
736026009,Elective lower segment cesarean section with bilateral tubal ligation
736118004,Emergency lower segment cesarean section with bilateral tubal ligation
737318003,Delayed hemorrhage due to and following miscarriage
737321001,Excessive hemorrhage due to and following molar pregnancy
737331008,Disorder of vein following miscarriage
73837001,Failed attempted abortion with cardiac arrest AND/OR failure
73972002,Postpartum neurosis
740597009,Umbilical cord complication during labor and delivery
74369005,Miscarriage with perforation of cervix
74437002,Ahlfeld's sign
74522004,"Cervical dilatation, 7cm"
74952004,"Gestation period, 3 weeks"
74955002,Retracted nipple associated with childbirth
74978008,Illegal abortion with complication
749781000000109,Incomplete termination of pregnancy
75013000,Legal abortion with perforation of vagina
75022004,"Gestational diabetes mellitus, class A>1<"
7504005,Trauma to vulva during delivery
75094005,Hydrops of placenta
75697004,"Term birth of identical twins, both stillborn"
75825001,Legal abortion with fat embolism
75928003,Pinard maneuver
75933004,Threatened abortion in second trimester
75947000,Legal abortion with endometritis
76012002,Fetal or neonatal effect of complication of labor and/or delivery
76037007,Rigid cervix uteri affecting pregnancy
762612009,Quadruplets with all four stillborn
762613004,Quintuplets with all five stillborn
762614005,Sextuplets with all six stillborn
76472002,Legal abortion with perforation of periurethral tissue
76771005,Parturient hemorrhage associated with hypofibrinogenemia
76871004,Previous surgery to vagina affecting pregnancy
76889003,Failed attempted abortion with cerebral anoxia
7707000,"Gestation period, 32 weeks"
77099001,Illegal abortion with perforation of uterus
77186001,Failed attempted abortion with renal tubular necrosis
77206006,Puerperal pelvic sepsis
77259008,Prolonged second stage of labor
77285007,Placental infarct affecting management of mother
77376005,Gestational edema without hypertension
77386006,Pregnant
77563000,Obstruction by bony pelvis
7768008,Failure of cervical dilation
77814006,Stillbirth of premature male (1000-2499 gms.)
77854008,Failed medical induction of labor
77913004,Submammary mastitis associated with childbirth
7792000,Placenta previa without hemorrhage
7802000,Illegal abortion without complication
7809009,Miscarriage with laceration of uterus
7822001,Failed attempted abortion complicated by damage to pelvic organs AND/OR tissues
78395001,"Gestation period, 33 weeks"
785341006,Intrapartum hemorrhage due to leiomyoma
785867009,Excessive hemorrhage due to and following induced termination of pregnancy
785868004,Secondary hemorrhage due to and following induced termination of pregnancy
785869007,Secondary hemorrhage due to and following illegally induced termination of pregnancy
785870008,Secondary hemorrhage due to and following legally induced termination of pregnancy
785871007,Excessive hemorrhage due to and following legally induced termination of pregnancy
785872000,Excessive hemorrhage due to and following illegally induced termination of pregnancy
7860005,Stillbirth of mature male (2500 gms. or more)
786067000,Intramural ectopic pregnancy of myometrium
78697003,Nonpurulent mastitis associated with childbirth
78808002,Essential hypertension complicating AND/OR reason for care during pregnancy
788180009,Lower uterine segment cesarean section
788290007,Hemangioma of skin in pregnancy
788728009,Hematoma of surgical wound following cesarean section
7888004,Term birth of newborn quadruplets
7910003,Miscarriage with salpingitis
79179003,Desultory labor
79255005,Mentum presentation of fetus
79290002,Cervical pregnancy
79414005,Retraction ring dystocia
79586000,Tubal pregnancy
796731000000105,Extra-amniotic injection of abortifacient
796741000000101,Intra-amniotic injection of abortifacient
79748007,Maternal dystocia
79839005,Perineal laceration involving vulva
79992004,"Gestation period, 12 weeks"
80002007,Malpresentation of fetus
80113008,Complication of the puerperium
80224003,Multiple gestation with one OR more fetal malpresentations
80228000,Lightening of fetus
80256005,Intervillous thrombosis
80438008,Illegal abortion with perforation of bladder
80487005,"Gestation period, 39 weeks"
8071005,Miscarriage with perforation of bowel
80722003,Surgical correction of inverted pregnant uterus
80818002,Previous surgery to perineum AND/OR vulva affecting pregnancy
80997009,Quintuplet pregnancy
81130000,Removal of intraligamentous ectopic pregnancy
81328008,Fundal dominance of uterine contraction
813541000000100,Pregnancy resulting from assisted conception
81448000,"Hemorrhage in early pregnancy, delivered"
81521003,Failed attempted abortion with salpingitis
816148008,Disproportion between fetus and pelvis due to conjoined twins
81677009,Lactation tetany
816966004,Augmentation of labor using oxytocin
816967008,Fetal distress due to augmentation of labor with oxytocin
816969006,Inefficient uterine activity with oxytocin augmentation
820947007,Third stage hemorrhage due to retention of placenta
82118009,"Gestation period, 2 weeks"
82153002,Miscarriage with pulmonary embolism
82204006,Illegal abortion with parametritis
82338001,Illegal abortion with septic shock
82661006,Abdominal pregnancy
82688001,Removal of ectopic fetus
82897000,"Spontaneous placental expulsion, Duncan mechanism"
83074005,Unplanned pregnancy
83094001,Perineal hematoma during delivery
83121003,"Term birth of fraternal twins, both stillborn"
83243004,Rigid pelvic floor affecting pregnancy
8333008,Term birth of newborn triplets
83916000,Postpartum thrombophlebitis
83922009,Miscarriage with parametritis
8393005,Tetanic contractions of uterus
84007008,Shock during AND/OR following labor AND/OR delivery
84032005,Halban's sign
840448004,Cystic hygroma in fetus co-occurrent with hydrops
840625002,Gravid uterus at 12-16 weeks size
840626001,Gravid uterus at 16-20 weeks size
840627005,Gravid uterus at 20-24 weeks size
840628000,Gravid uterus at 24-28 weeks size
840629008,Gravid uterus at 28-32 weeks size
840630003,Gravid uterus at 32-34 weeks size
840631004,Gravid uterus at 34-36 weeks size
840632006,Gravid uterus at 36-38 weeks size
840633001,Gravid uterus at term size
84132007,"Gestation period, 35 weeks"
84143004,Illegal abortion with perforation of cervix
84195007,Classical cesarean section
84235001,Cephalic version
84275009,Obstetrical hysterotomy
84382006,"Premature birth of fraternal twins, both stillborn"
8445003,Tumor of vulva affecting pregnancy
84457005,Spontaneous onset of labor
8468007,Legal abortion with uremia
84693004,Intervillous hemorrhage of placenta
85039006,Postpartum amenorrhea-galactorrhea syndrome
85116003,Miscarriage in second trimester
85331004,Miscarriage with laceration of broad ligament
85403009,"Delivery, medical personnel present"
854611000000109,Medically induced evacuation of retained products of conception using prostaglandin
85467007,Miscarriage with perforation of bladder
855021000000107,Ultrasonography of multiple pregnancy
855031000000109,Doppler ultrasonography of multiple pregnancy
85542007,Perineal laceration involving skin
85548006,Episiotomy
85632001,Miscarriage with laceration of vagina
85652000,Legal abortion with perforation of cervix
858901000000108,Pregnancy of unknown location
85991008,Miscarriage with pelvic peritonitis
860602007,Postpartum excision of uterus
86081009,Herpes gestationis
861281000000109,Antenatal 22 week examination
861301000000105,Antenatal 25 week examination
861321000000101,Antenatal 31 week examination
86196005,Disorder of breast associated with childbirth
86203003,Polyhydramnios
86356004,Unstable lie
863897005,Failed attempted termination of pregnancy complicated by acute necrosis of liver
86599005,"Echography, scan B-mode for placental localization"
866229004,Trauma to urethra and bladder during delivery
866481000000104,Ultrasonography to determine estimated date of confinement
8670007,Illegal abortion with salpingo-oophoritis
86801005,"Gestation period, 6 weeks"
86803008,Term birth of newborn quintuplets
86883006,"Gestation period, 23 weeks"
87038002,Postpartum alopecia
871005,Contracted pelvis
87178007,"Gestation period, 1 week"
87383005,Maternal distress
87527008,Term pregnancy
87605005,Cornual pregnancy
87621000,Hyperemesis gravidarum before end of 22 week gestation with dehydration
87662006,"Term birth of fraternal twins, both living"
87814002,Marginal placenta previa with intrapartum hemorrhage
87840008,Galactocele associated with childbirth
87967003,Miscarriage with perforation of vagina
87968008,Repair of old obstetrical laceration of cervix
88144003,Removal of ectopic interstitial uterine pregnancy requiring total hysterectomy
88178009,Puerperal peritonitis
88201000119101,Failure of cervical dilation due to primary uterine inertia
88362001,Removal of ectopic fetus from fallopian tube without salpingectomy
88697005,Papular dermatitis of pregnancy
8884000,Fetal ascites causing disproportion
88887003,Maternal hypotension syndrome
88895004,Fatigue during pregnancy
89053004,Vaginal cesarean section
89346004,Delivery by Kielland rotation
893721000000103,Defibulation of vulva to facilitate delivery
89672000,Retromammary mastitis associated with childbirth
89700002,Shoulder girdle dystocia
89849000,High forceps delivery
89934007,Crowning
8996006,Illegal abortion complicated by renal failure
90009001,Abnormal umbilical cord
90188009,Failed mechanical induction
90306000,Trial labor
904002,Pinard's sign
90438006,Delivery by Malstrom's extraction
90450000,Illegal abortion with pelvic peritonitis
90532005,Central laceration during delivery
90645002,Failed attempted abortion without complication
90797000,"Gestation period, 28 weeks"
90968009,Prolonged pregnancy
91162000,Necrosis of liver of pregnancy
9121000119106,Low back pain in pregnancy
91271004,Superfetation
91484005,Failure of induction of labor by oxytocic drugs
91957002,Back pain complicating pregnancy
921611000000101,Intrapartum stillbirth
9221009,Surgical treatment of septic abortion
92297008,Benign neoplasm of placenta
925561000000100,Gestation less than 28 weeks
92684002,Carcinoma in situ of placenta
9279009,Extra-amniotic pregnancy
9293002,Atony of uterus
9297001,Uterus bicornis affecting pregnancy
931004,"Gestation period, 9 weeks"
9343003,Term birth of newborn female
9442009,Parturient hemorrhage associated with afibrinogenemia
95606005,Maternal drug exposure
95607001,Maternal drug use
95608006,Necrosis of placenta
9686009,Goodell's sign
9720009,Cicatrix of cervix affecting pregnancy
9724000,Repair of current obstetric laceration of uterus
9780006,Presentation of prolapsed arm of fetus
9899009,Ovarian pregnancy

"""

# COMMAND ----------

# data = pd.read_csv(io.StringIO(preg
#                                + preg_del), header=0, delimiter=',').astype(stfer
from pyspark.sql import functions as F
preg_sdf = spark.createDataFrame(pd.read_csv(io.StringIO(preg), header=0, delimiter=',').astype(str)).withColumn('SOURCE', F.lit('primis-covid19-vacc-uptake-preg.csv'))
preg_del_sdf = spark.createDataFrame(pd.read_csv(io.StringIO(preg_del), header=0, delimiter=',').astype(str)).withColumn('SOURCE', F.lit('primis-covid19-vacc-uptake-pregdel.csv'))

# COMMAND ----------

preg_and_del_sdf = preg_sdf.union(preg_del_sdf)

# COMMAND ----------

# preg_sdf.filter(F.col('code') == 931004).show()
