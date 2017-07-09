REGISTER '/home/acadgild/project/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

A = LOAD '/user/acadgild/project/crimes.csv' USING CSVExcelStorage(',','NO_MULTILINE','UNIX');

B = FOREACH A GENERATE (chararray)$1 AS caseNum, (chararray)$4 AS iucr, (chararray)$14 AS fbiCode;

C = GROUP B BY fbiCode;

D = FOREACH C GENERATE group, COUNT(B.caseNum);

DUMP D;
