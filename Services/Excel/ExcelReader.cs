using ClosedXML.Excel;
using DataWhisperIngest.Services.Abstractions;
using Newtonsoft.Json;
using System.Text;
namespace DataWhisperIngest.Services.Excel;

public sealed class ExcelReader : IExcelReader
{
    public (List<string> headers, List<List<string>> rows) Read(string path)
    {
        string currentPath = System.IO.Directory.GetCurrentDirectory();
        var ext = Path.GetExtension(path).ToLowerInvariant();
        return ext switch
        {
            ".xlsx" or ".xlsm" or ".xltx" or ".xltm" => ReadExcel(path),
            ".csv" => ReadCsv(path),
            ".txt" => ReadPipeTxt(path, currentPath+"\\Services\\Mapping\\Reverse_Header_mapping.json"),
            _ => throw new NotSupportedException($"Extension '{ext}' is not supported.")
        };
    }

    private static (List<string>, List<List<string>>) ReadExcel(string path)
    {
        using var wb = new XLWorkbook(path);
        var ws = wb.Worksheet(1);

        var firstRow = ws.FirstRowUsed().RowNumber();
        var headerRow = ws.Row(firstRow);
        var headers = headerRow.CellsUsed().Select(c => c.GetString().Trim()).ToList();

        var rows = new List<List<string>>();
        var r = firstRow + 1;
        while (!ws.Row(r).IsEmpty())
        {
            var cells = ws.Row(r).Cells(1, headers.Count).Select(c => c.GetString()).ToList();
            if (cells.All(string.IsNullOrWhiteSpace)) break;
            rows.Add(cells);
            r++;
        }
        return (headers, rows);
    }

    private static (List<string>, List<List<string>>) ReadCsv(string path)
    {
        var headers = new List<string>();
        var rows = new List<List<string>>();
        int headerCount = 0;

        //var logPath = path + ".mismatch.log";
        using var reader = new StreamReader(path);
        //using var logWriter = new StreamWriter(logPath, append: true); // write logs to a separate file

        bool firstRecord = true;
        int dataRecordIndex = 0; // counts data rows (excludes header)
        var recordBuilder = new StringBuilder();

        // Local function: attempts to parse CSV record; returns (fields, isComplete)
        static (List<string> fields, bool isComplete) TryParseCsvRecord(string record)
        {
            var fields = new List<string>();
            var sb = new StringBuilder();
            bool inQuotes = false;

            for (int i = 0; i < record.Length; i++)
            {
                char c = record[i];

                if (c == '"')
                {
                    if (inQuotes)
                    {
                        // If next char is also a quote, it's an escaped quote ("")
                        if (i + 1 < record.Length && record[i + 1] == '"')
                        {
                            sb.Append('"');
                            i++; // skip next quote
                        }
                        else
                        {
                            // closing quote
                            inQuotes = false;
                        }
                    }
                    else
                    {
                        // opening quote (only if at start of field)
                        inQuotes = true;
                    }
                }
                else if (c == ',' && !inQuotes)
                {
                    fields.Add(sb.ToString());
                    sb.Clear();
                }
                else
                {
                    sb.Append(c);
                }
            }

            // End of record
            if (inQuotes)
            {
                // Not a complete record (we ended while still inside quotes)
                return (new List<string>(), false);
            }

            fields.Add(sb.ToString());
            return (fields, true);
        }

        // Helper to normalize potential BOM on the very first physical line
        static string TrimBom(string s) =>
            string.IsNullOrEmpty(s) ? s : s.TrimStart('\uFEFF');

        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            if (firstRecord && string.IsNullOrWhiteSpace(line))
            {
                // Skip leading blank lines before header
                continue;
            }

            // Append line to current record buffer (preserve newlines if a field spans multiple lines)
            if (recordBuilder.Length > 0)
                recordBuilder.Append('\n');

            recordBuilder.Append(line);

            // Try to parse the current accumulated record
            var current = recordBuilder.ToString();
            if (firstRecord)
                current = TrimBom(current);

            var (fields, isComplete) = TryParseCsvRecord(current);
            if (!isComplete)
            {
                // Need more lines to complete this record
                continue;
            }

            // We have a complete CSV record; clear the buffer for the next one
            recordBuilder.Clear();

            if (firstRecord)
            {
                headers = new List<string>(fields);
                headerCount = headers.Count;
                firstRecord = false;
            }
            else
            {
                dataRecordIndex++;
                rows.Add(fields);

                // Validate count against header
                if (fields.Count != headerCount)
                {
                    //logWriter.WriteLine(
                    //    $"Row #{dataRecordIndex + 1} (including header): " +
                    //    $"HeaderCount={headerCount}, RowCount={fields.Count}. " +
                    //    $"RawRecord={current}"
                    //);
                }
            }
        }

        // If file ended while inside an unterminated quoted record, note it
        if (recordBuilder.Length > 0)
        {
            // Try once more; if still incomplete, log it
            var current = recordBuilder.ToString();
            var (_, isComplete) = TryParseCsvRecord(current);
            if (!isComplete)
            {
                //logWriter.WriteLine("File ended with an unterminated quoted field. Partial record: " + current);
            }
        }

        return (headers, rows);
    }

    public (List<string> Headers, List<List<string>> Rows) ReadPipeTxt(string FilePath, string mappingPath)
    {
        // load mapping json
        var mappingJson = File.ReadAllText(mappingPath);
        var mapping = JsonConvert.DeserializeObject<MappingFile>(mappingJson);

        // build headers from mapping
        var headers = mapping.Columns
            .Select(c => c.Target?.Column ?? c.SourceHeaderGuess ?? $"Col{c.SourceIndex}")
            .ToList();

        var rows = new List<List<string>>();

        using var reader = new StreamReader(FilePath);
        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            if (string.IsNullOrWhiteSpace(line)) continue;

            var values = line.Split('|').Select(v => v.Trim()).ToList();

            // remove trailing empty column if last char was '|'
            if (values.Count > 0 && string.IsNullOrEmpty(values[^1]))
                values.RemoveAt(values.Count - 1);

            // align row length with headers
            while (values.Count < headers.Count)
                values.Add(string.Empty);

            rows.Add(values);
        }

        return (headers, rows);
    }

    public class MappingColumn
    {
        public int SourceIndex { get; set; }
        public string SourceHeaderGuess { get; set; }
        public TargetInfo Target { get; set; }
        public string Transform { get; set; }
        public double Confidence { get; set; }
    }
    public class TargetInfo
    {
        public string Table { get; set; }
        public string Column { get; set; }
    }

    public class MappingFile
    {
        public List<MappingColumn> Columns { get; set; }
    }
}
