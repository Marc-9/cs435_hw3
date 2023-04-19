

$HADOOP_HOME/bin/hadoop fs -rm -r /pageRank*

sbt package

if [ $? == 0 ]; then
    $SPARK_HOME/bin/spark-submit --class $1 --deploy-mode client --supervise target/scala-2.12/pagerank_2.12-0.1.jar /links /pageRanks
fi
