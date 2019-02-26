using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace KafkaConsumer
{
    class Program
    {
        const string SERVER = "192.168.42.132:9092";
        const string TOPIC = "Sheep";

        static void Main(string[] args)
        {
            Console.Title = "Consumer";

            Subscribe();

            Console.WriteLine("The End");
        }

        static void Subscribe()
        {
            using (var consumer = new Consumer<Ignore, string>(GetConfig()))
            {
                consumer.Subscribe(TOPIC);

                var assignment = consumer.Assignment;

                bool consuming = true;
                consumer.OnError += (_, e) => consuming = !e.IsFatal;
                //consumer.OnPartitionEOF += (_, topicPartitionOffset) =>
                //{
                //    Console.WriteLine(new string('_', 100));
                //    Console.WriteLine($"End of partition: {topicPartitionOffset}");
                //    Console.WriteLine(new string('_', 100));
                //};

                var counter = 0;

                while (consuming)
                {
                    try
                    {
                        var cr = consumer.Consume();
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        Thread.Sleep(200);

                        counter++;
                        var topicPartitions = consumer.Assignment;
                        var topicPartition = topicPartitions.Where(e => e.Partition == 1).FirstOrDefault();

                        if (counter == 50)
                        {
                            consumer.Seek(new TopicPartitionOffset(topicPartition, new Offset(30)));

                            //consumer.Pause(new List<TopicPartition> { topicPartition });
                        }

                        //if(counter == 500)
                        //{
                        //    consumer.Resume(new List<TopicPartition> { topicPartition });
                        //}
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }

                consumer.Close();
            }
        }

        static void Pause()
        {
            using (var consumer = new Consumer<Ignore, string>(GetConfig()))
            {
                try
                {
                    var assignment = consumer.Assignment;

                    var partition = new Partition(1);

                    var topicPartition = new TopicPartition(TOPIC, partition);

                    consumer.Pause(new List<TopicPartition> { topicPartition });

                    //consumer.Position(new List<TopicPartition> { topicPartition });

                    //var pos = consumer.Position(new List<TopicPartition> { topicPartition }).First();

                    //consumer.Seek(new TopicPartitionOffset(topicPartition, new Offset(50)));
                }
                catch (Exception ex)
                {
                    throw;
                }

                consumer.Close();
            }
        }

        static ConsumerConfig GetConfig()
        {
            var config = new ConsumerConfig
            {
                GroupId = "sheep-group",
                BootstrapServers = SERVER,
                AutoOffsetReset = AutoOffsetResetType.Earliest,
            };

            return config;
        }
    }
}
