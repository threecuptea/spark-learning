### spark-learing collected those small projects I learned from "Learning Spark: Lightning-Fast Big Data Analysis". Spark evolves a lot since then.  A lot of RDD operations can be replaced with Dataset/ DataFrame ones with much better performance.  DataFrameReader can be used to read csv, json etc. directly.  No need to parse ourselves.
#### I trimmed down this project to include only

    1. MyWordCount improves on top of classical WordCount.
        a) Filter out punctuations and stopWords
        b) Serves as a simple case to explain Spark basic processing elements: job, stage and task and their boundaries.

    2. BasicLoadNums: demostrate how to use accumulator correctly.
       Spark operations can be categorized into of transformation and action. Transformation operations are lazy
       by nature.  If an accumulator is incremented in transformation block, we have to use action like count()
       to trigger its execution before we can act on that accumulator.

    3. FileHadoopSaveLoad: demonstrate the load and save of hadoop files.
       a) Spark can access text, json, csv, parquet and orc files that we put in HDFS by using SparkSession.read.....
       b) However, We have to use newAPIHadoopFile to load those files, including Hive tables, saved by Hadoop                                                                         hive tables saved by Hadoop MapReduce, .
          MapReduce jobs.
       c) Typically, hadoop files are read using KeyValueTextInputFormat
       d) Key and value separator can be customized using org.apache.hadoop.conf.Configuration.

    4. FileSequenceSaveLoad
       a) No need to set InputFormat when reading sequence file.
       b) However, sequence file saved by by Hive with STORED AS SEQUENCEFILE is different from sequence file saved
          by Spark and needed to be handled differently

    5. FileLzoSaveLoad
       a) We load LZO files with newAPIHadoopFile by specifying com.hadoop.mapreduce.LzoTextInputFormat
       b) Since .LzoTextInputFormat is not part of Spark libraries, we have to specify it in spark-submit or
          spark-shell command line optons like the followings:

            bin/spark-submit --jars /usr/lib/hadoop/lib/hadoop-lzo.jar \
            --driver-library-path /usr/lib/hadoop/lib/native --driver-class-path /usr/lib/hadoop/lib

       c) Please refer to detailed comments in FileLzoSaveLoad to know but not limited to
          i) how to download hadoop lzo library and saved to the appropriate path
          ii) how to configure core-site.xml with additional lzop etc. codec
          iii) how to create lzo and lzo index files.








