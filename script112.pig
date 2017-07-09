REGISTER '/home/acadgild/project/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

A = LOAD '/user/acadgild/project/crimes.csv' USING CSVExcelStorage(',','NO_MULTILINE','UNIX');

B = FOREACH A GENERATE (chararray)$1 AS caseNum, (chararray)$4 AS iucr, (chararray)$14 AS fbiCode;

C = FILTER B BY fbiCode=='24';

D = GROUP C BY fbiCode;

E = FOREACH D GENERATE group, COUNT(C.caseNum);

DUMP E;

