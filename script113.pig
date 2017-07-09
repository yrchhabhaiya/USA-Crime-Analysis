REGISTER '/home/acadgild/project/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

A = LOAD '/user/acadgild/project/crimes.csv' USING CSVExcelStorage(',','NO_MULTILINE','UNIX');

B = FOREACH A GENERATE (chararray)$1 AS caseNum, (chararray)$5 AS iucr, (boolean)$8 AS arrest, (chararray)$11 AS dist;

C = FILTER B BY (arrest==true) AND (iucr=='THEFT' OR iucr=='MOTOR VEHICLE THEFT');

D = GROUP C BY dist;

E = FOREACH D GENERATE group, COUNT(C.caseNum);

DUMP E;

