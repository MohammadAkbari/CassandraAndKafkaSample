using Confluent.Kafka;
using System;

namespace KafkaSample
{
    class Program
    {
        static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "172.26.146.243:9092",
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var consumer = new Consumer<Ignore, string>(conf))
            {
                consumer.Subscribe("TutorialTopic");

                bool consuming = true;
                consumer.OnError += (_, e) => consuming = !e.IsFatal;

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

            Console.ReadKey();
        }

        private static void ProducerX()
        {
            var config = new ProducerConfig { BootstrapServers = "172.26.146.243:9092" };

            using (var producer = new Producer<Null, string>(config))
            {
                try
                {
                    var dr = producer.ProduceAsync("TutorialTopic", new Message<Null, string> { Value = "test" }).GetAwaiter().GetResult();
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
