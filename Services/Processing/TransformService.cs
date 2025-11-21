using DataWhisperIngest.Domain;
using DataWhisperIngest.Services.Abstractions;
namespace DataWhisperIngest.Services.Processing;

public sealed class TransformService : ITransformService
{
    public object? Apply(string? transform, string raw, string table, string column)
    {
        if (string.IsNullOrWhiteSpace(raw)) return DBNull.Value;

        var cs = DbSchema.GetColumn(table, column);
        var type = cs?.DataType?.ToLowerInvariant() ?? "";

        if (transform?.Contains("split first/last", StringComparison.OrdinalIgnoreCase) == true)
        {
            var parts = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (column.Equals("user_fstnm", StringComparison.OrdinalIgnoreCase)) return parts.FirstOrDefault();
            if (column.Equals("user_lstnm", StringComparison.OrdinalIgnoreCase)) return parts.Skip(1).LastOrDefault();
        }

        try
        {
            if (type.StartsWith("int")) return int.TryParse(raw, out var i) ? i : (object)DBNull.Value;
            if (type.StartsWith("float") || type.StartsWith("decimal"))
                return double.TryParse(raw, out var d) ? d : (object)DBNull.Value;

            if (type.StartsWith("bit"))
            {
                var v = raw.Trim().ToLowerInvariant();
                if (v is "1" or "true" or "yes" or "y") return true;
                if (v is "0" or "false" or "no" or "n") return false;
                return DBNull.Value;
            }

            if (type.StartsWith("datetime"))
            {
                if (DateTime.TryParse(raw, out var dt)) return dt;
                return DBNull.Value;
            }

            return raw; // nvarchar/other
        }
        catch { return DBNull.Value; }
    }

    public List<MappingItem> DedupeMappings(List<MappingItem> mappings, List<string> headers, string table)
    {
        bool IsExactOrAlias(string column, string header)
        {
            var cs = DbSchema.GetColumn(table, column);
            if (cs is null) return false;
            if (string.Equals(cs.Name, header, StringComparison.OrdinalIgnoreCase)) return true;
            return cs.Aliases?.Any(a => string.Equals(a, header, StringComparison.OrdinalIgnoreCase)) == true;
        }

        return mappings
            .GroupBy(m => m.Target!.Column, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.OrderByDescending(m => IsExactOrAlias(m.Target!.Column, m.SourceHeader))
                          .ThenByDescending(m => m.Confidence)
                          .ThenBy(m => headers.FindIndex(h => string.Equals(h, m.SourceHeader, StringComparison.OrdinalIgnoreCase)))
                          .First())
            .ToList();
    }
}
