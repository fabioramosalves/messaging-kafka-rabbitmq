using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

public class RabbitProducer : IRabbitProducer, IDisposable
{
    private readonly IConnection _connection;
    private readonly IModel _channel;

    public RabbitProducer(IConfiguration configuration)
    {
        var factory = new ConnectionFactory
        {
            HostName = configuration["Rabbit:Host"],
            UserName = configuration["Rabbit:User"],
            Password = configuration["Rabbit:Password"]
        };

        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
    }

    public Task PublishAsync<T>(
        string exchange,
        T message,
        CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        _channel.ExchangeDeclare(
            exchange: exchange,
            type: ExchangeType.Direct,
            durable: true);

        _channel.BasicPublish(
            exchange: exchange,
            routingKey: "",
            basicProperties: null,
            body: body);

        Console.WriteLine($"Mensagem enviada para exchange: {exchange}");

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _channel?.Close();
        _connection?.Close();
    }
}