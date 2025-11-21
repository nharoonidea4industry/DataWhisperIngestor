namespace DataWhisperIngest.Services.IO;

using DataWhisperIngest.Options;
using DataWhisperIngest.Services.Abstractions;
using Microsoft.Extensions.Options;

public sealed class FileArchiver : IFileArchiver
{
    private readonly IOptions<IngestOptions> _opt;
    public FileArchiver(IOptions<IngestOptions> opt)
    {
        _opt = opt;
    }

    public string Move(string sourcePath, string archiveRoot, int? ingestFileId = null)
    {
        Directory.CreateDirectory(archiveRoot);

        var incomingRoot = _opt.Value.IncomingPath; // or pass in as param
        var relativeDir = Path.GetRelativePath(incomingRoot, Path.GetDirectoryName(sourcePath)!);

        // build destination root (error or archive), preserving subfolder tree
        var destRoot = Path.Combine(archiveRoot, relativeDir);
        Directory.CreateDirectory(destRoot);

        // build unique destination filename
        var name = Path.GetFileNameWithoutExtension(sourcePath);
        var ext = Path.GetExtension(sourcePath);
        var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        var suffix = ingestFileId.HasValue
            ? $"__{ingestFileId.Value}__{stamp}"
            : $"__{stamp}";
        var dest = Path.Combine(destRoot, $"{name}{suffix}{ext}");

        int n = 1;
        while (File.Exists(dest))
        {
            dest = Path.Combine(destRoot, $"{name}{suffix}_{n}{ext}");
            n++;
        }

        File.Move(sourcePath, dest);
        return dest;
    }
}
