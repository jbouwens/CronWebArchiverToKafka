using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Cronos;
using FlareSolverrSharp.Solvers;
using Microsoft.Extensions.Logging;

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole()
        .SetMinimumLevel(LogLevel.Information);
});
var logger = loggerFactory.CreateLogger("WebScraper");
var configuration = LoadConfiguration();
var scraper = new WebScraper(logger, configuration.FlareSolverrUrl);

try
{
    await RunScrapingLoop(configuration, scraper);
}
catch (Exception ex)
{
    logger.LogCritical(ex, "Fatal error in scraping application");
    throw;
}
finally
{
    await scraper.CleanupAsync();
}
return;

async Task RunScrapingLoop(Configuration appConfig, WebScraper scraper)
{
    var now = DateTime.UtcNow;
    foreach (var item in appConfig.UrlsToScrape)
    {
        item.Initialize(now);
    }

    while (true)
    {
        var nextOccurrenceTime = appConfig.UrlsToScrape
            .Where(i => i.NextOccurrence.HasValue)
            .Select(i => i.NextOccurrence!.Value)
            .MinBy(x => x);

        if (nextOccurrenceTime == default)
        {
            logger.LogInformation("No more scheduled tasks. Exiting...");
            break;
        }

        var tasksToRun = appConfig.UrlsToScrape
            .Where(i => i.NextOccurrence == nextOccurrenceTime)
            .ToList();

        now = DateTime.UtcNow;
        var delay = nextOccurrenceTime - now;
        if (delay > TimeSpan.Zero)
        {
            logger.LogInformation("Next scheduled tasks at {NextRunTime} UTC. Waiting {Delay} seconds...",
                nextOccurrenceTime, delay.TotalSeconds);
            await Task.Delay(delay);
        }

        var scrapingTasks = tasksToRun.Select(item =>
            scraper.ScrapeAndSendAsync(item));
        await Task.WhenAll(scrapingTasks);

        now = DateTime.UtcNow;
        foreach (var item in tasksToRun)
        {
            item.UpdateNextOccurrence(now);
        }
    }
}

Configuration LoadConfiguration()
{
    var appSettingsPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "appsettings.json");
    if (File.Exists(appSettingsPath))
    {
        try
        {
            var deserialize = JsonSerializer.Deserialize<Configuration>(
                File.ReadAllText(appSettingsPath),
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            if (deserialize != null)
            {
                logger.LogInformation("Loaded configuration from {ConfigPath}", appSettingsPath);
                return deserialize;
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to load configuration from {ConfigPath}", appSettingsPath);
        }
    }

    var localSettingsPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "local.settings.json");
    var config = File.Exists(localSettingsPath)
        ? JsonSerializer.Deserialize<Configuration>(
              File.ReadAllText(localSettingsPath),
              new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
          ?? CreateDefaultConfiguration()
        : CreateDefaultConfiguration();

    return config;
}

Configuration CreateDefaultConfiguration()
{
    return new Configuration
    {
        FlareSolverrUrl = Environment.GetEnvironmentVariable("FlareSolverrUrl")
            ?? "http://localhost:8191",
        UrlsToScrape = new List<UrlQueueItem>()
    };
}

public class WebScraper(ILogger logger, string flareSolverrUrl)
{
    private readonly FlareSolverr _flareSolverr = new(flareSolverrUrl);
    private readonly HashSet<string> _managedSessions = new();
    private readonly Dictionary<string, string> _urlToSessionMap = new();

    public async Task ScrapeAndSendAsync(UrlQueueItem item)
    {
        try
        {
            if (item.ConnectionConfig == null)
                throw new InvalidOperationException("Event Hub connection not initialized for URL");

            logger.LogInformation("Starting scrape for {Url}", item.Url);
            var sessionId = await GetOrCreateSessionForUrlAsync(item.Url);
            var request = new HttpRequestMessage(HttpMethod.Get, new Uri(item.Url));
            var response = await _flareSolverr.Solve(request, sessionId);
            if (response.Status != "ok")
            {
                logger.LogError("Failed to get content for {Url}. Status: {Status}", item.Url, response.Status);
                return;
            }
            string contentType = response.Solution.Headers.ContentType ?? "text/html";
            await SendToEventHub(item, response.Solution.Response, contentType, item.ConnectionConfig);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing '{Url}'", item.Url);
        }
    }

    private async Task<string> GetOrCreateSessionForUrlAsync(string url)
    {
        if (_urlToSessionMap.TryGetValue(url, out var existingSession))
        {
            try
            {
                var testRequest = new HttpRequestMessage(HttpMethod.Get, new Uri(url));
                await _flareSolverr.Solve(testRequest, existingSession);
                return existingSession;
            }
            catch
            {
                _urlToSessionMap.Remove(url);
                _managedSessions.Remove(existingSession);
                logger.LogWarning("Session {SessionId} appears invalid, creating new one", existingSession);
            }
        }

        var response = await _flareSolverr.CreateSession();
        if (response.Status != "ok")
        {
            throw new Exception($"Failed to create session: {response.Message}");
        }

        var sessionId = response.Session;
        _urlToSessionMap[url] = sessionId;
        _managedSessions.Add(sessionId);
        logger.LogInformation("Created new FlareSolverr session: {SessionId}", sessionId);
        return sessionId;
    }

    private async Task SendToEventHub(UrlQueueItem item, string content, string contentType, EventHubConnectionConfig config)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config.BootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = config.SaslUsername,
            SaslPassword = config.SaslPassword,
            SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https,
            EnableIdempotence = true,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000,
            CompressionType = CompressionType.None,
            BatchSize = 16384,
            LingerMs = 0
        };

        using var producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();

        if (content.Contains("<pre"))
        {
            var startIndex = content.IndexOf("<pre");
            startIndex = content.IndexOf(">", startIndex) + 1;
            var endIndex = content.IndexOf("</pre>", startIndex);
            content = content.Substring(startIndex, endIndex - startIndex);
            contentType = "application/json";
        }

        byte[] messageBody;
        if (contentType.Contains("json"))
        {
            try
            {
                using var doc = JsonDocument.Parse(content);
                messageBody = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions
                {
                    WriteIndented = true
                }));
            }
            catch (JsonException)
            {
                logger.LogWarning("Content claimed to be JSON but failed to parse. Treating as plain text.");
                messageBody = Encoding.UTF8.GetBytes(content);
            }
        }
        else
        {
            messageBody = Encoding.UTF8.GetBytes(content);
        }

        var headers = new Headers
        {
            { "content-type", Encoding.UTF8.GetBytes(contentType) },
            { "source-url", Encoding.UTF8.GetBytes(item.Url) },
            { "scraped-at", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) }
        };

        var message = new Message<string, byte[]>
        {
            Key = item.Url,
            Value = messageBody,
            Headers = headers
        };

        var parts = config.SaslPassword.Split(';')
            .Select(part => part.Split(new[] { '=' }, 2))
            .Where(part => part.Length == 2)
            .ToDictionary(part => part[0], part => part[1]);

        if (!parts.TryGetValue("EntityPath", out var topicName))
            throw new ArgumentException("EntityPath not found in connection string");

        await producer.ProduceAsync(topicName, message);
        logger.LogInformation("Successfully sent {ContentType} content from '{Url}' to event hub '{EventHubName}'",
            contentType, item.Url, topicName);

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task CleanupAsync()
    {
        try
        {
            foreach (var session in _managedSessions)
            {
                await _flareSolverr.DestroySession(session);
                logger.LogInformation("Destroyed FlareSolverr session: {SessionId}", session);
            }
            _managedSessions.Clear();
            _urlToSessionMap.Clear();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error during session cleanup");
        }
    }
}

public class Configuration
{
    public string FlareSolverrUrl { get; set; } = string.Empty;
    public List<UrlQueueItem> UrlsToScrape { get; set; } = new();
}

public class EventHubConnectionConfig
{
    public string BootstrapServers { get; set; } = string.Empty;
    public string SaslUsername { get; set; } = string.Empty;
    public string SaslPassword { get; set; } = string.Empty;
}

public class UrlQueueItem
{
    public string Url { get; set; } = string.Empty;
    public string ConnectionString { get; set; } = string.Empty;
    public string CronExpression { get; set; } = string.Empty;

    [JsonIgnore] public CronExpression? Cron { get; private set; }
    [JsonIgnore] public DateTime? NextOccurrence { get; private set; }
    [JsonIgnore] public EventHubConnectionConfig? ConnectionConfig { get; private set; }

    public void Initialize(DateTime now)
    {
        Cron = Cronos.CronExpression.Parse(CronExpression);
        NextOccurrence = Cron.GetNextOccurrence(now);
        ConnectionConfig = ParseEventHubConnectionString(ConnectionString);
    }

    public void UpdateNextOccurrence(DateTime now)
    {
        NextOccurrence = Cron?.GetNextOccurrence(now);
    }

    private EventHubConnectionConfig ParseEventHubConnectionString(string connectionString)
    {
        var parts = connectionString.Split(';')
            .Select(part => part.Split(new[] { '=' }, 2))
            .Where(part => part.Length == 2)
            .ToDictionary(part => part[0], part => part[1]);

        if (!parts.TryGetValue("Endpoint", out var endpoint))
            throw new ArgumentException("Endpoint not found in connection string");

        var namespace_ = new Uri(endpoint).Host;

        return new EventHubConnectionConfig
        {
            BootstrapServers = $"{namespace_}:9093",
            SaslUsername = "$ConnectionString",
            SaslPassword = connectionString
        };
    }
}