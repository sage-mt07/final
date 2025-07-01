using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Tests.Integration;

public interface IKsqlClient
{
    Task<KsqlResponse> ExecuteExplainAsync(string ksql);
}

public record KsqlResponse(bool IsSuccess, string Message);

public class KsqlClient : IKsqlClient
{
    private readonly HttpClient _httpClient;

    public KsqlClient(Uri baseUri)
    {
        _httpClient = new HttpClient { BaseAddress = baseUri };
    }

    public async Task<KsqlResponse> ExecuteExplainAsync(string ksql)
    {
        var payload = new
        {
            ksql = $"EXPLAIN {ksql}",
            streamsProperties = new { }
        };
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await _httpClient.PostAsync("/ksql", content);
        var body = await response.Content.ReadAsStringAsync();
        var success = response.IsSuccessStatusCode && !body.Contains("\"error_code\"");
        return new KsqlResponse(success, body);
    }
}
