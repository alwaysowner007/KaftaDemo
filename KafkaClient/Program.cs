using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaCommon;
using Newtonsoft.Json;

namespace KafkaClient
{
    internal class Program
    {
        private static async Task Main()
        {
            Console.Title = "Client";

            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            while (true)
            {
                try
                {

                    var counter = new Random().Next(1, 1000000);
                    var item = new LotItem
                    {
                        Date = DateTime.UtcNow,
                        Contract = counter,
                        Lot = counter,
                        Price = counter * 100m,
                        User = $"{Guid.NewGuid()}"
                    };
                    var message = JsonConvert.SerializeObject(item);

                    await producer.ProduceAsync("my-topic", new Message<Null, string>
                    {
                        Value = message
                    });

                    Console.WriteLine(message);

                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }
    }
}
