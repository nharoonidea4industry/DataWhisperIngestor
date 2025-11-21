using System.Security.Cryptography;

namespace DataWhisperIngest.Utils;

public static class Hashing
{
    public static byte[] Sha256File(string path)
    {
        using var sha = SHA256.Create();
        using var s = File.OpenRead(path);
        return sha.ComputeHash(s);
    }
}
