namespace Producer.Worker;

public class KafkaWorker : BackgroundService
{
    private readonly ILogger<KafkaWorker> _logger;
    private readonly IKafkaProducer _kafkaProducer;

    public KafkaWorker(ILogger<KafkaWorker> logger, IKafkaProducer kafkaProducer)
    {
        _logger = logger;
        _kafkaProducer = kafkaProducer;
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

            _logger.LogInformation("Publishing order {Id} to Kafka", order.Id);
            await _kafkaProducer.PublishAsync("orders", order, stoppingToken);

            await Task.Delay(5000, stoppingToken);
        }
    }
}
