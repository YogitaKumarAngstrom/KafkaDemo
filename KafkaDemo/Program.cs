using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaDemo
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            //Writes topic on kafka
            CreateHostBuilder(args).Build().Run();
            //Read topics from kafka
            CreateHost1Builder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
     Host.CreateDefaultBuilder(args)
     .ConfigureServices((context, collection) =>
     {
         collection.AddHostedService<KafkaProducerHostedService>();
         // collection.AddHostedService<KafkaConsumerHandler>();
     });

        private static IHostBuilder CreateHost1Builder(string[] args) =>
     Host.CreateDefaultBuilder(args)
     .ConfigureServices((context, collection) =>
     {
         // collection.AddHostedService<KafkaProducerHostedService>();
         collection.AddHostedService<KafkaConsumerHandler>();
     });
    }

    public class KafkaProducerHostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> _logger;
        private IProducer<Null, string> _producer;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            _logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = " b-1.d1-msk.7dl64p.c3.kafka.eu-west-2.amazonaws.com:9092"
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                var s = "ASF";
                var rand = new Random();

                for (var i = 0; i < 5; i++)
                {
                    var value = s + rand.Next();
                    _logger.LogInformation(value);
                    await _producer.ProduceAsync("test-foo", new Message<Null, string>()
                    {
                        Value = value
                    }, cancellationToken);
                }
                _producer.Flush(TimeSpan.FromSeconds(10));
                //throw new NotImplementedException();
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR : " + e);
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
            //throw new NotImplementedException();
        }
    }

    public class KafkaConsumerHandler : IHostedService
    {
        private readonly string topic = "test-foo";

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "st_consumer_group",
                BootstrapServers = "b-1.d1-msk.7dl64p.c3.kafka.eu-west-2.amazonaws.com:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using (var builder = new ConsumerBuilder<Ignore,
                string>(conf).Build())
            {
                builder.Subscribe(topic);
                var cancelToken = new CancellationTokenSource();
                try
                {
                    while (true)
                    {
                        var consumer = builder.Consume(cancelToken.Token);
                        Console.WriteLine($"Message: {consumer.Message.Value} received from {consumer.TopicPartitionOffset}");
                    }
                }
                catch (Exception)
                {
                    builder.Close();
                }
            }
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}