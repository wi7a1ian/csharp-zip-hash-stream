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
var lfsPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
Directory.CreateDirectory(lfsPath);

{
    const int bufferSize = 8*1024; // i.e 10*1024*1024;
    using var memoStream =  new MemoryStream(bufferSize);
    using var nafStream = File.Create(Path.Combine(workPath, "out.naf"));
    using var cstream = new CryptoStream(nafStream, hasher, CryptoStreamMode.Write);
    using var zipArchive = new ZipArchive(cstream, ZipArchiveMode.Create);
    zipArchive.CreateEntry($"data/", CompressionLevel.SmallestSize);

    int i = 0;
    foreach(var finfo in Directory.GetFiles(@"./data", "*.txt").Select( f => new FileInfo(f)))
    {
        bool useLFS = finfo.Length > bufferSize;
        string tmpFilePath = Path.Combine(lfsPath, "tmp.buff");
        Stream tmpStream = useLFS ? File.Open(tmpFilePath, FileMode.Create) : memoStream;

        fileCopyRetryPolicy.Execute( () => {
            tmpStream.Position = 0;
            tmpStream.SetLength(0); // zero alloc reset
            using var fstream = finfo.Open(FileMode.Open, FileAccess.Read);
            if(i++%7 == 0) throw new IOException("Oh noes!"); // CHAOS MONKEYYY!
            fstream.CopyTo(tmpStream);
        });

        var entry = zipArchive.CreateEntry($"data/{finfo.Name}", CompressionLevel.SmallestSize);
        using var entryStream = entry.Open();
        tmpStream.Position = 0;
        tmpStream.CopyTo(entryStream);

        if(useLFS)
        {
            tmpStream.Close();
            File.Delete(tmpFilePath);
        }
    }
}

var calculatedHash = Convert.ToBase64String(hasher.Hash);
File.WriteAllText(Path.Combine(workPath, "out.naf.hash"), calculatedHash);

var fileHash = Convert.ToBase64String(hasher.ComputeHash(File.OpenRead(Path.Combine(workPath, "out.naf"))));
Debug.Assert(fileHash == calculatedHash);


