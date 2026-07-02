using Consumer.Worker;
using Serilog;
using Serilog.Sinks.Elasticsearch;

var builder = Host.CreateApplicationBuilder(args);

Serilog.Debugging.SelfLog.Enable(Console.Error.WriteLine);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Application", "Worker")
    .WriteTo.Console()
    .WriteTo.Elasticsearch(new ElasticsearchSinkOptions(new Uri("http://localhost:9200"))
    {
        AutoRegisterTemplate = true,
        AutoRegisterTemplateVersion = AutoRegisterTemplateVersion.ESv8,
        IndexFormat = "worker-logs-{0:yyyy.MM.dd}",
        TypeName = null,
        EmitEventFailure = EmitEventFailureHandling.WriteToSelfLog
    })
    .CreateLogger();

builder.Logging.AddSerilog(Log.Logger);


builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
builder.Services.AddSingleton<IRabbitConsumer, RabbitConsumer>();
builder.Services.AddHostedService<KafkaWorker>();
builder.Services.AddHostedService<RabbitWorker>();

builder.Logging.ClearProviders();
builder.Logging.AddSerilog(Log.Logger);

var host = builder.Build();
host.Run();
