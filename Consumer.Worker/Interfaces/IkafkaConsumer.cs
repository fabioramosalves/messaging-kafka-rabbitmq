public interface IKafkaConsumer
{
    Task ConsumeAsync(CancellationToken cancellationToken = default);
}