using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Query.Ksql;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Ksql;

public class KsqlDbRestApiClientTests
{
    private class Handler : HttpMessageHandler
    {
        private readonly HttpResponseMessage _response;
        public Handler(HttpResponseMessage response) => _response = response;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => Task.FromResult(_response);
    }

    [Fact]
    public async Task ExecuteQueryAsync_ParsesResponse()
    {
        var json = "{\"header\":{\"schema\":[{\"name\":\"ID\"}]}}\n{\"row\":{\"columns\":[1]}}";
        var message = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json) };
        var client = new KsqlDbRestApiClient("http://unit", new HttpClient(new Handler(message)));
        var result = await client.ExecuteQueryAsync("select * from t");
        Assert.Single(result.Header);
        Assert.Single(result.Rows);
        Assert.Equal(1, System.Convert.ToInt32(result.Rows[0]["ID"]));
    }

    [Fact]
    public async Task ExecuteStatementAsync_ParsesResponse()
    {
        var json = "{\"statementText\":\"CREATE STREAM\",\"commandId\":\"1\"}";
        var message = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json) };
        var client = new KsqlDbRestApiClient("http://unit", new HttpClient(new Handler(message)));
        var result = await client.ExecuteStatementAsync("create stream");
        Assert.Equal("CREATE STREAM", result.StatementText);
        Assert.Equal("1", result.CommandId);
    }
}
