using StackExchange.Redis;
using System;

namespace Redis1
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var redis = ConnectionMultiplexer.Connect("localhost"))
            {
                IDatabase db = redis.GetDatabase();

                for (int i = 0; i < 500000; i++)
                {
                    db.ListLeftPush("mykey", $"v.1.{i}");
                }
            }

            Console.WriteLine("Hello World!");
        }
    }
}
