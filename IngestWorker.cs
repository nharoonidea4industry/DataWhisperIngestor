using DataWhisperIngest.Options;
using DataWhisperIngest.Services.Abstractions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataWhisperIngest.Services.Workers;

public sealed class IngestWorker : BackgroundService
{
    private readonly ILogger<IngestWorker> _log;
    private readonly IngestOptions _opt;
    private readonly IFileProcessor _processor;
    private readonly IErrorLogger _err;

    public IngestWorker(
        ILogger<IngestWorker> log,
        IOptions<IngestOptions> opt,
        IFileProcessor processor, IErrorLogger err)
    {
        _log = log;
        _opt = opt.Value;
        _processor = processor;
        _err = err;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string _Currentfilepath = "";
        _log.LogInformation("Worker started. Watching {Path}", _opt.IncomingPath);
        Directory.CreateDirectory(_opt.IncomingPath);
        Directory.CreateDirectory(_opt.ArchiveFolder);
        Directory.CreateDirectory(_opt.ErrorFolder);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var files = Directory.EnumerateFiles(_opt.IncomingPath, "*.*", SearchOption.AllDirectories)
                                     .Where(f => f.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase)
                                              || f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)
                                              || f.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
                                     .ToList();

                foreach (var f in files)
                {
                    _Currentfilepath = f;
                    await _processor.ProcessAsync(f, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Top-level loop error.");
                await using var conn = new SqlConnection(_opt.SqlConnectionString);
                await conn.OpenAsync(stoppingToken);
                await using var tx = conn.BeginTransaction();
                await _err.LogAsync(_opt.SqlConnectionString, 0, "Ingest Worker Reading File", ex, _Currentfilepath, Path.GetFileName(_Currentfilepath));
            }

            await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, _opt.PollSeconds)), stoppingToken);
        }
    }
}
