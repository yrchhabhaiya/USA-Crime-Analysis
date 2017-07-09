REGISTER '/home/acadgild/project/piggybank.jar';

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

A = LOAD '/user/acadgild/project/crimes.csv' USING CSVExcelStorage(',','NO_MULTILINE','UNIX');

B = FOREACH A GENERATE (chararray)$2 AS date, (boolean)$8 AS arrest;

C = FOREACH B GENERATE ToDate(date, 'MM/dd/yyyy HH:mm:ss aa') AS date, arrest;

D = FILTER C BY (arrest==true) AND date<ToDate('2015-10-31T00:00:00.000+05:30') AND date>ToDate('2014-09-30T00:00:00.000+05:30');

E = GROUP D BY arrest;

F = FOREACH E GENERATE COUNT(D.arrest);

DUMP F;

