DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
school = load '/user/tj763/DOE_High_School_Directory_2014-2015.csv' USING CSVExcelStorage(',') 
	AS (f1:chararray,f2:chararray,f3:chararray,f4:chararray,f5:chararray,f6:chararray,f7:chararray,f8:chararray,f9:chararray,
    f10:chararray,f11:chararray,f12:chararray,f13:chararray,f14:chararray,f15:chararray,f16:chararray,f17:chararray,f18:int);

ranked = rank school;
df = FILTER ranked by (rank_school % 3 == 2);
school_new = FILTER df BY (f18 >= 500);
 

sat = load '/user/tj763/SAT_Results.csv' USING CSVExcelStorage(',') 
	AS (f1:chararray,f2:chararray,f3:int,f4:chararray,f5:int);
sat_new = FILTER sat BY f5 is not null;


df = JOIN school_new BY f1, sat_new BY f1;

df2 = FOREACH df GENERATE school_new::f3, sat_new::f5;

df3 = GROUP df2 BY f3;


avg_sat = FOREACH df3 GENERATE group, AVG(df2.f5);

dump avg_sat

dump avg_sat