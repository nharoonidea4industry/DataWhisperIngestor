using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataWhisperIngest.Domain
{
    public sealed record ColumnSchema(string Name, string DataType, string[] Aliases, bool Insertable = true);
    public sealed record TableSchema(string Table, List<ColumnSchema> Columns);
    public sealed record MappingTarget(string Table, string Column);
    public sealed record MappingItem(string SourceHeader, MappingTarget? Target, double Confidence, string? Transform);
    public sealed record MappingResult(List<MappingItem> Columns);
}
