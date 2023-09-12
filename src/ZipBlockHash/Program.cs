using System.Diagnostics;
using System.IO.Compression;
using System.Reflection;
using System.Security.Cryptography;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;

static bool IsRequestedCancellation(Exception e) => 
			e is OperationCanceledException c 
			&& c.CancellationToken.IsCancellationRequested;

var delay = Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromSeconds(1), retryCount: 5);
RetryPolicy fileCopyRetryPolicy = Policy
				.Handle<Exception>(e => !IsRequestedCancellation(e))
				.WaitAndRetry(delay, (exception, timespan) => Console.WriteLine(exception.Message));

using var hasher = SHA512.Create();
var workPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

{
    const int bufferSize = 8*1024; // i.e 10*1024*1024;
    var buffer = new byte[bufferSize];
    using var nafStream = File.Create(Path.Combine(workPath, "out.naf"));
    using var cstream = new CryptoStream(nafStream, hasher, CryptoStreamMode.Write);
    using var zipArchive = new ZipArchive(cstream, ZipArchiveMode.Create);
    zipArchive.CreateEntry($"data/", CompressionLevel.SmallestSize);

    int i = 0;
    foreach(var finfo in Directory.GetFiles(@"./data", "*.txt").Select( f => new FileInfo(f)))
    {
        var entry = zipArchive.CreateEntry($"data/{finfo.Name}", CompressionLevel.SmallestSize);
        using var entryStream = entry.Open();
        using var fileStream = finfo.Open(FileMode.Open, FileAccess.Read);

        int bytesRead = 0;
        do 
        {
            bytesRead = fileCopyRetryPolicy.Execute( () => {
                if(i++%7 == 0) throw new IOException("Oh noes!"); // CHAOS MONKEYYY!
                return fileStream.Read(buffer.AsSpan());
            });

            if (bytesRead > 0) 
            {
                entryStream.Write(buffer, 0, bytesRead);
            }
        } while (bytesRead > 0);
    }
}

var calculatedHash = Convert.ToBase64String(hasher.Hash);
File.WriteAllText(Path.Combine(workPath, "out.naf.hash"), calculatedHash);

var fileHash = Convert.ToBase64String(hasher.ComputeHash(File.OpenRead(Path.Combine(workPath, "out.naf"))));
Debug.Assert(fileHash == calculatedHash);


