using DataWhisperIngest.Options;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWhisperIngest.Services.Abstractions
{
    public interface IExcelReader
    {
        (List<string> headers, List<List<string>> rows) Read(string path);
    }

    public interface IMapperService
    {
        Task<Domain.MappingResult> MapAsync(
            string apiKey, List<string> headers, List<Domain.TableSchema> dbSchema, string model, CancellationToken ct,string path);
    }

    public interface ITransformService
    {
        object? Apply(string? transform, string raw, string table, string column);
        List<Domain.MappingItem> DedupeMappings(List<Domain.MappingItem> mappings, List<string> headers, string table);
    }

    public interface IIngestRepository
    {
        Task<int> GetOrCreateIngestFileIdAsync(SqlConnection conn, SqlTransaction tx, string path, int rowsCount);
        Task<string> GetCustomerMappingfilePath(SqlConnection conn, SqlTransaction tx, string path);
        Task MarkFailedAsync(SqlConnection conn, SqlTransaction tx, int ingestFileId);
        Task SetFileStatusAsync(SqlConnection conn, SqlTransaction? tx, int ingestFileId, string status, bool? failed = null);
        Task ExecuteMainTables(SqlConnection conn, SqlTransaction? tx, int ingestFileId);
        Task InsertRowAsync(SqlConnection conn, SqlTransaction tx, string table, IEnumerable<(string Col, object? Val)> values);
    }

    public interface IFileArchiver
    {
        string Move(string sourcePath, string destinationRoot, int? ingestFileId = null);
    }

    public interface IErrorLogger
    {
        Task<int> LogAsync(string opt, int ingestFileId, string stage, Exception ex, string filePath, string Filename,
            int? excelRowNumber = null, string? sourceHeader = null, string? targetField = null, string severity = "Error");
        //Task LogRowAsync(SqlConnection conn, SqlTransaction? tx, object batchId, object lineNo, string v, object mapped);
    }

    public interface IFileProcessor
    {
        Task ProcessAsync(string path, CancellationToken ct);
    }
}
