public interface IRabbitProducer
{
    Task PublishAsync<T>(
        string queueOrExchange,
        T message,
        CancellationToken cancellationToken = default);
}