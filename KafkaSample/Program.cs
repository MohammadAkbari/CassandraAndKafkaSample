using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;

namespace KafkaSample
{
    class Program
    {
        const string SERVER = "192.168.20.80:9092";
        const string TOPIC = "Sheep4";

        static void Main(string[] args)
        {
            Console.Title = "Producer";

            var config = new ProducerConfig
            {
                BootstrapServers = SERVER,
                //PartitionAssignmentStrategy = PartitionAssignmentStrategyType.Roundrobin,
                //Partitioner = PartitionerType.Random
            };

            using (var producer = new Producer<Null, string>(config))
            {
                using (var adminClient = new AdminClient(producer.Handle))
                {
                    adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = TOPIC, NumPartitions = 5, ReplicationFactor = 1 } }).Wait();
                    adminClient.CreatePartitionsAsync(new List<PartitionsSpecification> { new PartitionsSpecification { Topic = TOPIC, IncreaseTo = 6 } }).Wait();
                }
            }


            for (int i = 0; i < 100000; i++)
            {
                ProducerX(TOPIC, i);
            }

            Console.WriteLine(new string('_', 200));

            Console.ReadKey();
        }

        private static void ConsumerX()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = SERVER,
                AutoOffsetReset = AutoOffsetResetType.Earliest,
            };

            using (var consumer = new Consumer<Ignore, string>(conf))
            {
                consumer.Subscribe("TutorialTopic");

                bool consuming = true;
                consumer.OnError += (_, e) => consuming = !e.IsFatal;
                consumer.OnPartitionEOF += (_, topicPartitionOffset) =>
                {
                    Console.WriteLine($"End of partition: {topicPartitionOffset}");
                };

                while (consuming)
                {
                    try
                    {
                        var cr = consumer.Consume();
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }

                consumer.Close();
            }
        }

        private static void ProducerX(string topic, int value)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = SERVER,
                //PartitionAssignmentStrategy = PartitionAssignmentStrategyType.Roundrobin,
                //Partitioner = PartitionerType.Random
            };

            using (var producer = new Producer<Null, string>(config))
            {
                try
                {
                    var key = value % 5;

                    var partition = new Partition(key);

                    var topicPartition = new TopicPartition(topic, partition);

                    var message = new Message<Null, string> { Value = $"value-{value}" };

                    var dr2 = producer.ProduceAsync(topic, message).GetAwaiter().GetResult();

                    var dr = producer.ProduceAsync(topicPartition, message).GetAwaiter().GetResult();

                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }


    //public interface IBookingConsumer
    //{
    //    void Listen(Action<string> message);
    //}

    //public class BookingConsumer : IBookingConsumer
    //{
    //    public void Listen(Action<string> message)
    //    {
    //        var config = new Dictionary<string, object>
    //        {
    //            { "group.id","booking_consumer" },
    //            { "bootstrap.servers", "172.26.146.243:9092" },
    //            { "enable.auto.commit", "false" }
    //        };

    //        using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
    //        {
    //            consumer.Subscribe("TutorialTopic");
    //            consumer.OnMessage += (_, msg) => {
    //                message(msg.Value);
    //            };

    //            while (true)
    //            {
    //                consumer.Poll(100);
    //            }
    //        }
    //    }
    //}
}
