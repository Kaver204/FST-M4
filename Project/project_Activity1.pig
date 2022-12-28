--Project Activity1

-- Load input file from HDFS
inputFileSpeech = LOAD 'hdfs:///user/root/projectFiles' USING PigStorage('\t') AS (name:chararray, speech:chararray);
--filter the first 2 lines
ranked = RANK inputFileSpeech ;
dialogues = FILTER ranked BY (rank_inputFileSpeech  >2);
-- group by 
grpd = GROUP dialogues BY name;
-- Count the number of names
cntd = FOREACH grpd GENERATE $0 as name , COUNT(inputFileSpeech.name) AS numOfLines;
-- Order by Name
orderName = ORDER cntd BY numOfLines DESC;
-- Store the result in HDFS
STORE orderName INTO 'hdfs:///user/root/Outputs/dialogueOutput_1' USING PigStorage('\t');
