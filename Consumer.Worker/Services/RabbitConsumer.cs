using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public class RabbitConsumer : IRabbitConsumer, IDisposable
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly ILogger<RabbitConsumer> _logger;
    private readonly string _queue;

    public RabbitConsumer(IConfiguration configuration, ILogger<RabbitConsumer> logger)
    {
        _logger = logger;
        var factory = new ConnectionFactory
        {
            HostName = configuration["Rabbit:Host"],
            UserName = configuration["Rabbit:User"],
            Password = configuration["Rabbit:Password"]
        };

        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();

        var exchange = configuration["Rabbit:Exchange"]!;
        _queue = configuration["Rabbit:Queue"]!;

        _channel.ExchangeDeclare(
            exchange: exchange,
            type: ExchangeType.Direct,
            durable: true);

        _channel.QueueDeclare(
            queue: _queue,
            durable: true,
            exclusive: false,
            autoDelete: false);

        _channel.QueueBind(
            queue: _queue,
            exchange: exchange,
            routingKey: "");

        _channel.BasicQos(0, 1, false);
    }

    public async Task ConsumeAsync(CancellationToken cancellationToken = default)
    {
        var consumer = new EventingBasicConsumer(_channel);

        consumer.Received += (sender, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var order = JsonSerializer.Deserialize<OrderCreated>(message);

                Console.WriteLine("--------------------------------");
                Console.WriteLine($"Queue.....: {_queue}");
                Console.WriteLine($"Id........: {order?.Id}");
                Console.WriteLine($"Value.....: {order?.Value}");
                Console.WriteLine($"Date......: {order?.Date}");
                Console.WriteLine("--------------------------------");

                _channel.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro ao processar mensagem");

                _channel.BasicNack(ea.DeliveryTag, false, true);
            }
        };

        _channel.BasicConsume(
            queue: _queue,
            autoAck: false,
            consumer: consumer);

        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        _channel?.Close();
        _connection?.Close();
    }
}