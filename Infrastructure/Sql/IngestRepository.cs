using Dapper;
using DataWhisperIngest.Services;
using DataWhisperIngest.Services.Abstractions;
using DataWhisperIngest.Utils;
using DocumentFormat.OpenXml.Wordprocessing;
using Microsoft.Data.SqlClient;
using System.Data;

namespace DataWhisperIngest.Infrastructure.Sql;

public sealed class IngestRepository : IIngestRepository
{
    public async Task<int> GetOrCreateIngestFileIdAsync(SqlConnection conn, SqlTransaction tx, string path, int rowsCount)
    {
        var hash = Hashing.Sha256File(path);

        var existing = await conn.ExecuteScalarAsync<int?>(
            "SELECT IngestFileId FROM dbo.IngestFile WHERE FileHash = @h",
            new { h = hash }, tx);
        if (existing.HasValue) return existing.Value;

        var id = await conn.ExecuteScalarAsync<int>(
            @"INSERT INTO dbo.IngestFile (FileName, FilePath, FileHash, FilesRowCount)
              OUTPUT INSERTED.IngestFileId
              VALUES (@FileName, @FilePath, @FileHash, @FilesRowCount);",
            new
            {
                FileName = Path.GetFileName(path),
                FilePath = path,
                FileHash = hash,
                FilesRowCount = rowsCount
            }, tx);

        return id;
    }

    public async Task MarkFailedAsync(SqlConnection conn, SqlTransaction tx, int ingestFileId)
    {
        await conn.ExecuteAsync(
            @"UPDATE dbo.IngestFile SET Is_Failed = 1 WHERE IngestFileId = @ingestFileId",
            new { ingestFileId }, tx);
    }

    public async Task SetFileStatusAsync(SqlConnection conn, SqlTransaction? tx, int ingestFileId, string status, bool? failed = null)
    {
        var sql = @"
UPDATE dbo.IngestFile
   SET Status = @Status,
       Is_Failed = COALESCE(@Failed, Is_Failed),
       CompletedAt = CASE WHEN @Status IN ('Completed','Failed') THEN SYSUTCDATETIME() ELSE CompletedAt END,
       UpdatedAt = SYSUTCDATETIME()
 WHERE IngestFileId = @Id;";
        await conn.ExecuteAsync(sql, new { Status = status, Failed = failed, Id = ingestFileId }, tx);
    }

    public async Task InsertRowAsync(SqlConnection conn, SqlTransaction tx, string table, IEnumerable<(string Col, object? Val)> values)
    {
        var cols = values.Select(v => v.Col).ToList();
        var paramNames = cols.Select(c => "@" + c).ToList();

        var sql = $"INSERT INTO [{table}] ({string.Join(",", cols.Select(c => $"[{c}]"))}) VALUES ({string.Join(",", paramNames)})";
        var dp = new DynamicParameters();
        foreach (var (col, val) in values) dp.Add("@" + col, val);

        await conn.ExecuteAsync(sql, dp, tx, commandType: CommandType.Text);
    }

    public async Task<string> GetCustomerMappingfilePath(SqlConnection conn, SqlTransaction tx, string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        // 2) Use Path APIs (handles both \ and /); avoid manual Split()
        var id = Path.GetFileNameWithoutExtension(path);
        id = id.Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        id = id.Split('_', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        if (string.IsNullOrWhiteSpace(id))
            return string.Empty;

        // 3) Escape LIKE pattern & parameterize
        var escaped = EscapeLike(id);
        var pattern = $"%{escaped}%";

        const string sql = @"
SELECT TOP (1) MappingFilePath
FROM dbo.vu_MainEntList
WHERE MappingFilePath LIKE @pattern ESCAPE '\';";

        var existing = await conn.ExecuteScalarAsync<string?>(
            sql, new { pattern }, tx);

        return existing ?? string.Empty;

    }

    public async Task ExecuteMainTables(SqlConnection conn, SqlTransaction? tx, int ingestFileId)
    {
        var sql = @"
        EXEC dbo.usp_run_all_upserts @Id; ";
        await conn.ExecuteAsync(sql, new { Id = ingestFileId }, tx);
    }

    private string EscapeLike(string input)
    {
        // Escape SQL LIKE wildcards and the escape char itself
        return input
            .Replace(@"\", @"\\")
            .Replace("%", @"\%")
            .Replace("_", @"\_")
            .Replace("[", @"\[");
    }

    
}
