using Confluent.Kafka;
using System;
using System.Threading;

namespace KafkaConsumer
{
    class Program
    {
        const string SERVER = "192.168.20.80:9092";
        const string TOPIC = "Sheep4";

        static void Main(string[] args)
        {
            Console.Title = "Consumer";

            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = SERVER,
                AutoOffsetReset = AutoOffsetResetType.Earliest,
            };

            using (var consumer = new Consumer<Ignore, string>(conf))
            {
                consumer.Subscribe(TOPIC);

                bool consuming = true;
                consumer.OnError += (_, e) => consuming = !e.IsFatal;
                //consumer.OnPartitionEOF += (_, topicPartitionOffset) =>
                //{
                //    Console.WriteLine($"End of partition: {topicPartitionOffset}");
                //};

                while (consuming)
                {
                    try
                    {
                        var cr = consumer.Consume();
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        //Thread.Sleep(2000);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }

                consumer.Close();
            }

            Console.WriteLine("The End");
        }
    }
}
