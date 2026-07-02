using System.Text.Json;
using Confluent.Kafka;

public class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer(IConfiguration configuration)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"]
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishAsync<T>(
        string topic,
        T message,
        CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(message);

        var kafkaMessage = new Message<string, string>
        {
            Key = Guid.NewGuid().ToString(),
            Value = json
        };

        var result = await _producer.ProduceAsync(
            topic,
            kafkaMessage,
            cancellationToken);

        Console.WriteLine(
            $"Mensagem enviada para {result.TopicPartitionOffset}");
    }

    public void Dispose()
    {
        _producer.Flush();
        _producer.Dispose();
    }
}