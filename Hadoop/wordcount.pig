inputFile = LOAD 'hdfs:///user/kaveri/pigInput.txt' AS (lines);

--tokenize each work (map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(lines)) AS word;

--group words
grpd = GROUP words BY word;

--count the number of words(reduce)
tot_count = FOREACH grpd GENERATE group,COUNT(words);

--Store the result in HDFS
STORE tot_count INTO 'hdfs:///user/kaveri/pigOutput';
