
using Producer.Worker;
using Serilog;
using Serilog.Sinks.Elasticsearch;

Serilog.Debugging.SelfLog.Enable(Console.Error.WriteLine);

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Application", "Worker")
    .WriteTo.Console()
    .WriteTo.Elasticsearch(new ElasticsearchSinkOptions(new Uri("http://localhost:9200"))
    {
        AutoRegisterTemplate = false,
        IndexFormat = "worker-logs-{0:yyyy.MM.dd}",
        TypeName = null,
        EmitEventFailure = EmitEventFailureHandling.WriteToSelfLog
    })
    .CreateLogger();

builder.Logging.AddSerilog(Log.Logger);

builder.Services.AddHostedService<KafkaWorker>();
builder.Services.AddHostedService<RabbitWorker>();
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IRabbitProducer, RabbitProducer>();

var host = builder.Build();

try
{
    host.Run();
}
finally
{
    Log.CloseAndFlush();
}
