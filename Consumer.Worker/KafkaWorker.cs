namespace Consumer.Worker;

public class KafkaWorker : BackgroundService
{
    private readonly ILogger<KafkaWorker> _logger;
    private readonly IKafkaConsumer _kafkaConsumer;

    public KafkaWorker(ILogger<KafkaWorker> logger, IKafkaConsumer kafkaConsumer)
    {
        _logger = logger;
        _kafkaConsumer = kafkaConsumer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("KafkaWorker starting...");

        await _kafkaConsumer.ConsumeAsync(stoppingToken);

        _logger.LogInformation("KafkaWorker stopping.");
    }
}
