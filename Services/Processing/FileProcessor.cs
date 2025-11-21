using DataWhisperIngest.Domain;
using DataWhisperIngest.Options;
using DataWhisperIngest.Services.Abstractions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace DataWhisperIngest.Services.Processing;

public sealed class FileProcessor : IFileProcessor
{
    private readonly ILogger<FileProcessor> _log;
    private readonly IOptions<IngestOptions> _opt;
    private readonly IExcelReader _reader;
    private readonly IMapperService _mapper;
    private readonly ITransformService _transform;
    private readonly IIngestRepository _repo;
    private readonly IFileArchiver _archiver;
    private readonly IErrorLogger _err;

    public FileProcessor(
        ILogger<FileProcessor> log,
        IOptions<IngestOptions> opt,
        IExcelReader reader,
        IMapperService mapper,
        ITransformService transform,
        IIngestRepository repo,
        IFileArchiver archiver,
        IErrorLogger err)
    {
        _log = log;
        _opt = opt;
        _reader = reader;
        _mapper = mapper;
        _transform = transform;
        _repo = repo;
        _archiver = archiver;
        _err = err;
    }

    public async Task ProcessAsync(string path, CancellationToken ct)
    {
        int ingestFileId = 0;
        List<string> apis = new List<string>();
        apis.Add("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");
        apis.Add("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");
        apis.Add("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");
        //var apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        Random r = new Random();
        int index = r.Next(apis.Count);
        var apiKey = apis[index];
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            _log.LogError("OPENAI_API_KEY not set; skipping file {File}", path);
            _archiver.Move(path, _opt.Value.ErrorFolder);
            return;
        }
        _log.LogInformation(path);
        var (headers, rows) = _reader.Read(path);
        if (rows.Count == 0 || headers.Count == 0)
        {
            _log.LogWarning("No data/headers found in {File}; moving to error.", path);
            _archiver.Move(path, _opt.Value.ErrorFolder);
            return;
        }
        _log.LogInformation("Headers :{headers.Count} Rows: {rows.Count}", headers.Count, rows.Count);
        await using var conn = new SqlConnection(_opt.Value.SqlConnectionString);
        await conn.OpenAsync(ct);
        await using var tx = conn.BeginTransaction();

        ingestFileId = await _repo.GetOrCreateIngestFileIdAsync(conn, tx, path, rows.Count);

        MappingResult mapping;
        try
        {
            string CustomerMappingFilePath = await _repo.GetCustomerMappingfilePath(conn, tx, path);
            mapping = await _mapper.MapAsync(apiKey, headers, DbSchema.Tables, _opt.Value.Model, ct, CustomerMappingFilePath);

        }
        catch (Exception ex)
        {
            _log.LogError(ex, "OpenAI mapping failed for {File}", path);
            _archiver.Move(path, _opt.Value.ErrorFolder);
            await _err.LogAsync(_opt.Value.SqlConnectionString,ingestFileId, "Insert", ex, path, Path.GetFileName(path));
            return;
        }

        var usable = mapping.Columns
            .Where(c => c.Target is not null)
            //.Where(c => c.Target is not null && c.Confidence >= _opt.Value.MinConfidence)
            .ToList();

        var byTable = usable
            .Where(m => DbSchema.GetColumn(m.Target!.Table, m.Target!.Column)?.Insertable == true)
            .GroupBy(m => m.Target!.Table, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                g => g.Key,
                g => _transform.DedupeMappings(g.ToList(), headers, g.Key),
                StringComparer.OrdinalIgnoreCase);

        

        
        try
        {
            
            _log.LogInformation("IngestFileId = {Id}", ingestFileId);
            await _repo.SetFileStatusAsync(conn, tx, ingestFileId, "Processing");

            foreach (var (table, tableMappings) in byTable)
            {
                int rownumber = 0;
                foreach (var row in rows)
                {
                    rownumber++;
                    var values = new List<(string Col, object? Val)>();

                    var usedCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var m in tableMappings)
                    {
                        if (!usedCols.Add(m.Target!.Column)) continue;
                        var idx = headers.FindIndex(h => string.Equals(h, m.SourceHeader, StringComparison.OrdinalIgnoreCase));
                        if (idx < 0) continue;

                        var raw = row.ElementAtOrDefault(idx);
                        if (string.IsNullOrWhiteSpace(raw)) raw = "0";

                        var value = _transform.Apply(m.Transform, raw!, m.Target!.Table, m.Target!.Column);
                        values.Add((m.Target!.Column, value));
                    }

                    // IngestFileId
                    values.Add(("IngestFileId", ingestFileId));

                    // ExcelRowNumber if present
                    if (DbSchema.GetColumn("stgMain", "ExcelRowNumber")?.Insertable == true)
                        values.Add(("ExcelRowNumber", rownumber));

                    if (values.Count == 0) continue;

                    await _repo.InsertRowAsync(conn, tx, table, values);

                    var percentage = (rownumber * 100) / rows.Count;
                    Console.SetCursorPosition(0, Console.CursorTop);
                    Console.Write($"Inserted record {rownumber} -- {percentage}%");
                }
            }

            await _repo.SetFileStatusAsync(conn, tx, ingestFileId, "Completed", failed: false);
            //await _repo.ExecuteMainTables(conn, null, ingestFileId);
            await tx.CommitAsync(ct);

            try
            {
                var archived = _archiver.Move(path, _opt.Value.ArchiveFolder, ingestFileId);
                _log.LogInformation("Archived {File} -> {Dest}", path, archived);
            }
            catch (Exception moveEx)
            {
                _log.LogWarning(moveEx, "Insert OK but archiving failed for {File}. Moving to error.", path);
                _archiver.Move(path, _opt.Value.ErrorFolder, ingestFileId);
                await _repo.SetFileStatusAsync(conn, null, ingestFileId, "Failed", failed: true);
            }
        }
        catch (Exception ex)
        {
            await tx.RollbackAsync(ct);
            _log.LogError(ex, "Insert failed for {File}. Moving to error.", path);
            _archiver.Move(path, _opt.Value.ErrorFolder, ingestFileId == 0 ? null : ingestFileId);
            if (ingestFileId != 0)
            {
                await _repo.MarkFailedAsync(conn, tx, ingestFileId);
                await _repo.SetFileStatusAsync(conn, tx, ingestFileId, "Failed", failed: true);
                await _err.LogAsync(_opt.Value.SqlConnectionString, ingestFileId, "Insert", ex, path, Path.GetFileName(path));
            }
            
        }
    }
}
