using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;

namespace KafkaSample
{
    class Program
    {
        const string SERVER = "192.168.42.132:9092";
        const string TOPIC = "Sheep";

        static void Main(string[] args)
        {
            Console.Title = "Producer";

            DeleteTopic();
            CreateTopic();

            for (int i = 0; i < 10000; i++)
            {
                PublisherWithoutPartition(TOPIC, i);
            }

            Console.WriteLine(new string('_', 100));

            Console.ReadKey();
        }

        private static void DeleteTopic()
        {
            using (var producer = new Producer<Null, string>(GetConfig()))
            {
                using (var adminClient = new AdminClient(producer.Handle))
                {
                    try
                    {
                        adminClient.DeleteTopicsAsync(new List<string> { TOPIC }).Wait();
                    }
                    catch (Exception ex)
                    {
                    }
                }
            }
        }

        private static void CreateTopic()
        {
            using (var producer = new Producer<Null, string>(GetConfig()))
            {
                using (var adminClient = new AdminClient(producer.Handle))
                {
                    adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = TOPIC, NumPartitions = 5, ReplicationFactor = 1 } }).Wait();
                    adminClient.CreatePartitionsAsync(new List<PartitionsSpecification> { new PartitionsSpecification { Topic = TOPIC, IncreaseTo = 6 } }).Wait();
                }
            }
        }

        private static ProducerConfig GetConfig()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = SERVER,
                PartitionAssignmentStrategy = PartitionAssignmentStrategyType.Range,
                Partitioner = PartitionerType.Consistent
            };

            return config;
        }

        private static void PublisherWithoutPartition(string topic, int value)
        {
            using (var producer = new Producer<int, string>(GetConfig(), Serializers.Int32))
            {
                try
                {
                    var message = new Message<int, string> { Key = value, Value = $"value-{value}" };

                    var dr = producer.ProduceAsync(topic, message).GetAwaiter().GetResult();

                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

        private static void PublisherWithPartition(string topic, int value)
        {
            using (var producer = new Producer<Null, string>(GetConfig()))
            {
                try
                {
                    var key = value % 5;

                    var partition = new Partition(key);

                    var topicPartition = new TopicPartition(topic, partition);

                    var message = new Message<Null, string> { Value = $"value-{value}" }; 

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
