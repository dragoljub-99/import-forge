using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace ImportForge.Infrastructure.Storage;

public sealed class ImportFileStorage
{
    private const string SolutionFileName = "ImportForge.sln";
    private readonly string _uploadsRootPath;

    public ImportFileStorage(IConfiguration configuration, IHostEnvironment environment)
    {
        var uploadsRootSetting = configuration["ImportForge:UploadsRoot"];
        _uploadsRootPath = ResolveUploadsRootFullPath(uploadsRootSetting, environment.ContentRootPath);
        Directory.CreateDirectory(_uploadsRootPath);
    }

    public async Task SaveAsync(long jobId, Stream content, CancellationToken ct)
    {
        var filePath = GetFilePath(jobId);
        await using var destination = File.Create(filePath);
        await content.CopyToAsync(destination, ct);
    }

    public bool Exists(long jobId)
        => File.Exists(GetFilePath(jobId));

    public Stream OpenRead(long jobId)
        => File.OpenRead(GetFilePath(jobId));

    private string GetFilePath(long jobId)
        => Path.Combine(_uploadsRootPath, $"{jobId}.csv");

    private static string ResolveUploadsRootFullPath(string? uploadsRootSetting, string contentRootPath)
    {
        var setting = string.IsNullOrWhiteSpace(uploadsRootSetting) ? "./uploads" : uploadsRootSetting.Trim();

        if (Path.IsPathRooted(setting))
        {
            return Path.GetFullPath(setting);
        }

        var solutionRoot = FindSolutionRoot(contentRootPath) ?? contentRootPath;
        return Path.GetFullPath(setting, solutionRoot);
    }

    private static string? FindSolutionRoot(string startDirectory)
    {
        var current = new DirectoryInfo(startDirectory);

        while (current is not null)
        {
            var slnPath = Path.Combine(current.FullName, SolutionFileName);
            if (File.Exists(slnPath))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return null;
    }
}
