using Dapper;
using DataWhisperIngest.Options;
using DataWhisperIngest.Services;
using DataWhisperIngest.Services.Abstractions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace DataWhisperIngest.Infrastructure.Logging;

public sealed class IngestErrorLogger : IErrorLogger
{
    public async Task<int> LogAsync(string SqlConnectionString, int ingestFileId, string stage, Exception ex, string filePath, string Filename,
        int? excelRowNumber = null, string? sourceHeader = null, string? targetField = null, string severity = "Error")
    {
        await using var conn = new SqlConnection(SqlConnectionString);
        await conn.OpenAsync();
        await using var tx = conn.BeginTransaction();
        try
        {
            var p = new
            {
                IngestFileId = ingestFileId,
                Stage = stage,
                ExcelRowNumber = excelRowNumber,
                SourceHeader = sourceHeader,
                TargetField = targetField,
                ErrorCode = ex.HResult.ToString(),
                Severity = severity,
                Message = ex.Message,
                Details = ex.ToString(),
                FilePath = filePath,
                FileName = Filename
            };

            var sql = @"
INSERT INTO dbo.IngestError
  ( Stage, ExcelRowNumber, SourceHeader, TargetField, ErrorCode, Severity, Message, Details,FilePath,FileName)
VALUES
  (@Stage, @ExcelRowNumber, @SourceHeader, @TargetField, @ErrorCode, @Severity, @Message, @Details,@FilePath,@FileName);";
            return await conn.ExecuteAsync(sql, p, tx);
        }
        catch(Exception exs)
        {
            throw;
        }

 //       UPDATE dbo.IngestFile
 //          SET ErrorCount = ErrorCount + 1,
 //      LastErrorMessage = LEFT(@Message, 2000),
 //      UpdatedAt = SYSUTCDATETIME()
 //WHERE IngestFileId = @IngestFileId;
    }

    public async Task LogRowAsync(SqlConnection conn, SqlTransaction? tx,object batchId,object lineNo,string message,Dictionary<string, object?> mapped,string stage = "RowValidation",string? sourceHeader = null,  string? targetField = null,string errorCode = "VAL001",string severity = "Error")    
    {
        await conn.OpenAsync();

        var sql = @"
INSERT INTO dbo.IngestError
  (IngestFileId, Stage, ExcelRowNumber, SourceHeader, TargetField, ErrorCode, Severity, Message, Details)
VALUES
  (@IngestFileId, @Stage, @ExcelRowNumber, @SourceHeader, @TargetField, @ErrorCode, @Severity, @Message, @Details);

UPDATE dbo.IngestFile
   SET ErrorCount = ErrorCount + 1,
       LastErrorMessage = LEFT(@Message, 2000),
       UpdatedAt = SYSUTCDATETIME()
 WHERE IngestFileId = @IngestFileId;";

        var p = new
        {
            IngestFileId = batchId,
            Stage = stage,
            ExcelRowNumber = lineNo,
            SourceHeader = sourceHeader,
            TargetField = targetField,
            ErrorCode = errorCode,
            Severity = severity,
            Message = message,
            Details = JsonSerializer.Serialize(mapped) // snapshot of row
        };

        await conn.ExecuteAsync(sql, p);
    }
}
