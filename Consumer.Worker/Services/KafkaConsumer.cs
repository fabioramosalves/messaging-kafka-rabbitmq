using System.Text.Json;
using Confluent.Kafka;

public class KafkaConsumer : IKafkaConsumer, IDisposable
{
    private readonly IConsumer<string, string> _consumer;

    public KafkaConsumer(IConfiguration configuration)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            GroupId = configuration["Kafka:GroupId"],
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();

        _consumer.Subscribe(configuration["Kafka:Topic"]);
    }

    public async Task ConsumeAsync(CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(cancellationToken);

                var order = JsonSerializer.Deserialize<OrderCreated>(
                    consumeResult.Message.Value);

                Console.WriteLine("--------------------------------");
                Console.WriteLine($"Topic.....: {consumeResult.Topic}");
                Console.WriteLine($"Partition.: {consumeResult.Partition}");
                Console.WriteLine($"Offset....: {consumeResult.Offset}");
                Console.WriteLine($"Key.......: {consumeResult.Message.Key}");
                Console.WriteLine($"Id........: {order?.Id}");
                Console.WriteLine($"Value.....: {order?.Value}");
                Console.WriteLine($"Date......: {order?.Date}");
                Console.WriteLine("--------------------------------");

                await Task.CompletedTask;
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }
}