using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;
using System;
using System.Collections.Generic;
using System.Text;

namespace MobiusSample
{
    class Program
    {
        static void Main(string[] args)
        {
            var checkpointPath = "";
            var sparkContext = new SparkContext(new SparkConf());
            var slideDurationInMillis = 10;
            var topics = new List<string>();
            var kafkaParams = new List<Tuple<string, string>>();
            var perTopicPartitionKafkaOffsets = new List<Tuple<string, long>>();
            var windowDurationInSecs = 10;
            var slideDurationInSecs = 10;

            StreamingContext sparkStreamingContext = StreamingContext.GetOrCreate(checkpointPath, () =>
            {
                var ssc = new StreamingContext(sparkContext, slideDurationInMillis);
                ssc.Checkpoint(checkpointPath);
                var stream = KafkaUtils.CreateDirectStream(ssc, topics, kafkaParams, perTopicPartitionKafkaOffsets);

                var countByLogLevelAndTime = stream
                                              .Map(kvp => Encoding.UTF8.GetString(kvp.Item2))
                                              .Filter(line => line.Contains(","))
                                              .Map(line => line.Split(','))
                                              .Map(columns => new Tuple<string, int>(
                                                                    string.Format("{0},{1}", columns[0], columns[1]), 1))
                                              .ReduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y,
                                                                    windowDurationInSecs, slideDurationInSecs, 3)
                                              .Map(logLevelCountPair => string.Format("{0},{1}",
                                                                    logLevelCountPair.Item1, logLevelCountPair.Item2));
                countByLogLevelAndTime.ForeachRDD(countByLogLevel =>
                {
                    foreach (var logCount in countByLogLevel.Collect())
                        Console.WriteLine(logCount);
                });
                return ssc;
            });

            sparkStreamingContext.Start();
            sparkStreamingContext.AwaitTermination();

            Console.WriteLine("Hello World!");
        }
    }
}
