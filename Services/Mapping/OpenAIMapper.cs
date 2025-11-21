using DataWhisperIngest.Domain;
using DataWhisperIngest.Services.Abstractions;
using DocumentFormat.OpenXml.Spreadsheet;
using OpenAI.Chat;
using System.Text.Json;

namespace DataWhisperIngest.Services.Mapping;

public sealed class OpenAIMapper : IMapperService
{
    public async Task<MappingResult> MapAsync(
        string apiKey,
        List<string> headers,
        List<TableSchema> dbSchema,
        string model,
        CancellationToken ct,string path)
    {
        #region jsonBased Mapping
        string currentPath = System.IO.Directory.GetCurrentDirectory();
        string filePath = "";
        filePath = currentPath + path;

        string resjson = await File.ReadAllTextAsync(filePath);

        // Tell JsonSerializer which type to deserialize
        var result = JsonSerializer.Deserialize<MappingResult>(resjson, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
        }
        );
        return result;

        #endregion
        #region OPenAI
        //var client = new ChatClient(model: model, apiKey: apiKey);

        //var system = new SystemChatMessage(
        //    """
        //    You are a data-mapping assistant. Map each Excel header to a {table,column} from the provided database schema.
        //    - Only choose among the provided tables/columns.
        //    - If unsure, set target to null and confidence low.
        //    - Prefer exact/alias matches; consider likely transforms (e.g., "Full Name" -> split first/last).
        //    - Output STRICT JSON that matches the provided JSON Schema.
        //    """
        //);

        //var payload = new { schema = dbSchema, excel_headers = headers };
        //var user = new UserChatMessage(JsonSerializer.Serialize(payload));

        //var mappingSchema =
        //    """
        //    {
        //      "type":"object",
        //      "properties":{
        //        "columns":{
        //          "type":"array",
        //          "items":{
        //            "type":"object",
        //            "properties":{
        //              "SourceHeader":{"type":"string"},
        //              "Target":{
        //                "anyOf":[
        //                  {"type":"object","properties":{"Table":{"type":"string"},"Column":{"type":"string"}},"required":["Table","Column"],"additionalProperties":false},
        //                  {"type":"null"}
        //                ]
        //              },
        //              "Confidence":{"type":"number","minimum":0.0,"maximum":1.0},
        //              "Transform":{"type":"string"}
        //            },
        //            "required":["SourceHeader","Target","Confidence","Transform"],
        //            "additionalProperties":false
        //          }
        //        }
        //      },
        //      "required":["columns"],
        //      "additionalProperties":false
        //    }
        //    """;

        //var options = new ChatCompletionOptions
        //{
        //    ResponseFormat = ChatResponseFormat.CreateJsonSchemaFormat(
        //        jsonSchemaFormatName: "column_mapping",
        //        jsonSchema: BinaryData.FromString(mappingSchema),
        //        jsonSchemaIsStrict: true),
        //    MaxOutputTokenCount = 800,
        //};

        //// ---------- Timeouts ----------
        //// How long to allow each attempt to run before timing out:
        //var perAttemptTimeout = TimeSpan.FromSeconds(12);

        //// Absolute wall-clock deadline for the whole MapAsync (across retries):
        //var overallTimeout = TimeSpan.FromSeconds(60);

        //// Link the caller's token with an overall timeout:
        //using var overallCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        //overallCts.CancelAfter(overallTimeout);

        //ChatCompletion? completion = null;
        //int attempts = 0;
        //int maxAttempts = 3;

        //while (true)
        //{
        //    // Stop if overall deadline already hit
        //    overallCts.Token.ThrowIfCancellationRequested();

        //    try
        //    {
        //        // Create a per-attempt timeout linked to the overall/caller token
        //        using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(overallCts.Token);
        //        attemptCts.CancelAfter(perAttemptTimeout);

        //        completion = await client.CompleteChatAsync(
        //            messages: new ChatMessage[] { system, user },
        //            options: options,
        //            cancellationToken: attemptCts.Token
        //        );

        //        break; // success
        //    }
        //    catch (OperationCanceledException) when (!ct.IsCancellationRequested && attempts < maxAttempts)
        //    {
        //        // If the caller didn't cancel, this was a timeout (either per-attempt or overall).
        //        // If overall already canceled, rethrow to surface a clean timeout.
        //        if (overallCts.IsCancellationRequested)
        //        {

        //            throw new TimeoutException($"OpenAI call exceeded overall timeout of {overallTimeout.TotalSeconds:N0}s.");
        //        }

        //        attempts++;
        //        if (attempts >= maxAttempts)
        //        {
        //            //string currentPath = System.IO.Directory.GetCurrentDirectory();
        //            //string filePath = "";
        //            //filePath = currentPath + path;

        //            //string resjson = await File.ReadAllTextAsync(filePath);

        //            //// Tell JsonSerializer which type to deserialize
        //            //return JsonSerializer.Deserialize<MappingResult>(resjson, new JsonSerializerOptions
        //            //{
        //            //    PropertyNameCaseInsensitive = true,
        //            //}
        //            //);
        //        }//throw new TimeoutException($"OpenAI call timed out after {attempts} attempt(s) of {perAttemptTimeout.TotalSeconds:N0}s each (overall limit {overallTimeout.TotalSeconds:N0}s).");

        //        // small backoff before retrying
        //        var backoff = TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempts));
        //        await Task.Delay(backoff, overallCts.Token);
        //    }
        //    catch when (++attempts <= maxAttempts)
        //    {
        //        // Non-timeout transient error: retry with exponential backoff
        //        var backoff = TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempts));
        //        await Task.Delay(backoff, overallCts.Token);
        //    }

        //    if (attempts > maxAttempts)
        //    {

        //    throw new TimeoutException($"OpenAI call failed after {maxAttempts} attempt(s) within {overallTimeout.TotalSeconds:N0}s.");
        //    }
        //}

        //var json = completion!.Content[0].Text;

        //var result = JsonSerializer.Deserialize<MappingResult>(json,
        //    new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
        //    ?? new MappingResult(new());

        //return result;

        #endregion
    }
}
