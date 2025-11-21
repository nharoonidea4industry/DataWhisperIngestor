using DataWhisperIngest.Infrastructure.Logging;
using DataWhisperIngest.Infrastructure.Sql;
using DataWhisperIngest.Options;
using DataWhisperIngest.Services.Abstractions;
using DataWhisperIngest.Services.Excel;
using DataWhisperIngest.Services.IO;
using DataWhisperIngest.Services.Mapping;
using DataWhisperIngest.Services.Processing;
using DataWhisperIngest.Services.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<IngestOptions>(builder.Configuration.GetSection("Ingest"));
builder.Services.AddSingleton<IExcelReader, ExcelReader>();
builder.Services.AddSingleton<IFileArchiver, FileArchiver>();
builder.Services.AddSingleton<IMapperService, OpenAIMapper>();
builder.Services.AddSingleton<ITransformService, TransformService>();
builder.Services.AddSingleton<IIngestRepository, IngestRepository>();
builder.Services.AddSingleton<IErrorLogger, IngestErrorLogger>();
builder.Services.AddSingleton<IFileProcessor, FileProcessor>();
builder.Services.AddHostedService<IngestWorker>();

await builder.Build().RunAsync();
