using System;
using System.Threading;
using Confluent.Kafka;
using KafkaCommon;
using Newtonsoft.Json;

namespace KafkaServer
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.Title = "Server";

            var config = new ConsumerConfig
            {
                GroupId = "my-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var build = new ConsumerBuilder<Ignore, string>(config).Build();
           
            build.Subscribe("my-topic");

            while (true)
            {
                try
                {
                    var consume = build.Consume();
                    var message = consume.Message.Value;
                    var item = JsonConvert.DeserializeObject<LotItem>(message);
                    var timeNow = DateTime.UtcNow;

                    Console.WriteLine(timeNow.Subtract(item.Date).ToString("g"));
                }
                catch ( Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }
    }
}
