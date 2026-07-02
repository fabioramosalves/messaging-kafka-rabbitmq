namespace Consumer.Worker;

public class RabbitWorker : BackgroundService
{
    private readonly ILogger<RabbitWorker> _logger;
    private readonly IRabbitConsumer _rabbitConsumer;

    public RabbitWorker(ILogger<RabbitWorker> logger, IRabbitConsumer rabbitConsumer)
    {
        _logger = logger;
        _rabbitConsumer = rabbitConsumer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("RabbitWorker starting...");

        await _rabbitConsumer.ConsumeAsync(stoppingToken);

        _logger.LogInformation("RabbitWorker stopping.");
    }
}
