using StackExchange.Redis;
using System;

namespace Redis2
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
                    db.ListLeftPush("mykey", $"v.2.{i}");
                }
            }

            Console.WriteLine("Hello World!");
        }
    }
}
