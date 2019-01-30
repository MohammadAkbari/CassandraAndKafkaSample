using Cassandra;
using System;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

namespace CassandraSample
{
    class Program
    {
        static void Main(string[] args)
        {
            var cluster = Cluster.Builder()
                     .AddContactPoints("172.26.146.247")
                     //.AddContactPoints("192.168.20.201")
                     //.AddContactPoints("192.168.20.203")
                     .WithCredentials("cassandra", "cassandra")
                     .Build();
                        MappingConfiguration.Global.Define<MyMappings>();

            var session = cluster.Connect("space");

            //Linq(session);

            //for (int i = 0; i < 5; i++)
            //{
            //    InsertLinq(session);
            //}


            //UpdateById(session);


            //var users = new Table<User>(session);

            //var id = TimeUuid.Parse("6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47");

            //users.Where(u => u.FristName == "xyz")
            //  .Select(u => new User { LastName = "abc" })
            //  .Update()
            //  .Execute();

            PrintCountCql(session);

           // PrintListCql(session);

            Console.ReadLine();
        }

        private static void UpdateById(ISession session)
        {
            var users = new Table<User>(session);

            var id = TimeUuid.Parse("6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47");

            users.Where(u => u.Id == id)
              .Select(u => new User { FristName = "xyz" })
              .Update()
              .Execute();
        }

        private static void InsertLinq(ISession session)
        {
            var users = new Table<User>(session);

            var random = new Random();
            var number = random.Next(1, 10000);

            users.Insert(new User
            {
                Id = TimeUuid.NewId(),
                FristName = $"f {number}",
                LastName = $"l {number}"
            }).Execute();
        }

        private static void Linq(ISession session)
        {
            var users = new Table<User>(session);

            User user = (from x in users select x).FirstOrDefault().Execute();
        }

        private static void PrintListCql(ISession session)
        {
            var result = session.Execute("SELECT id, firstname, lastname FROM user");

            foreach (var row in result)
            {
                var id = row.GetValue<Guid>("id");
                var lastName = row.GetValue<string>("lastname");
                var firstName = row.GetValue<string>("firstname");
                Console.WriteLine($"{id} - {firstName} - {lastName}");
            }
        }

        private static void PrintCountCql(ISession session)
        {
            var result = session.Execute("SELECT COUNT(*) FROM user");

            foreach (var row in result)
            {
                var count = row.GetValue<long>("count");
                Console.WriteLine($"Count: {count}");
            }
        }
    }

    public class MyMappings : Mappings
    {
        public MyMappings()
        {
            For<User>()
               .TableName("user")
               .PartitionKey(u => u.Id)
               .Column(u => u.Id, cm => cm.WithName("id"))
               .Column(c=>c.FristName, cm=> cm.WithName("firstname"))
               .Column(c => c.LastName, cm => cm.WithName("lastname"));
        }
    }

    public class User
    {
        public TimeUuid Id { get; set; }
        public string FristName { get; set; }
        public string LastName { get; set; }
    }
}
