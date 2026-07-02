public interface IRabbitConsumer
{
    Task ConsumeAsync(CancellationToken cancellationToken = default);
}