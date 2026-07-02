namespace Producer.Worker;

public class RabbitWorker : BackgroundService
{
    private readonly ILogger<RabbitWorker> _logger;
    private readonly IRabbitProducer _rabbitProducer;

    public RabbitWorker(ILogger<RabbitWorker> logger, IRabbitProducer rabbitProducer)
    {
        _logger = logger;
        _rabbitProducer = rabbitProducer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var order = new OrderCreated
            {
                Id = Guid.NewGuid(),
                Value = 100,
                Date = DateTime.UtcNow
            };

            _logger.LogInformation("Publishing order {Id} to RabbitMQ", order.Id);
            await _rabbitProducer.PublishAsync("orders", order, stoppingToken);

            await Task.Delay(5000, stoppingToken);
        }
    }
}
