using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWhisperIngest.Services.Processing
{
    public interface IWhisperService
    {
        Task GenerateAsync(SqlConnection conn, SqlTransaction tx, int ingestFileId, CancellationToken ct);
    }

    public sealed class WhisperService : IWhisperService
    {
        public async Task GenerateAsync(SqlConnection conn, SqlTransaction tx, int ingestFileId, CancellationToken ct)
        {
            // Example: exec a stored proc that inspects MainEntSett and suppresses channels
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "EXEC dbo.sp_GenerateWhispers @IngestFileId";
            cmd.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@IngestFileId", ingestFileId));
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
