using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Query.Ksql;
using Xunit;

namespace Kafka.Ksql.Linq.Core.Tests;

public class KsqlKBuilderApplicationTests
{
    private class Handler : HttpMessageHandler
    {
        private readonly HttpResponseMessage _response;
        public Handler(HttpResponseMessage response) => _response = response;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => Task.FromResult(_response);
    }

    [Fact]
    public async Task ExecuteStreamingQueryAsync_ParsesRows()
    {
        var lines = "{\"header\":{\"schema\":[{\"name\":\"ID\"}]}}\n{\"row\":{\"columns\":[1]}}\n{\"finalMessage\":\"x\"}";
        var message = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(lines, Encoding.UTF8) };
        var client = new KsqlDbRestApiClient("http://unit", new HttpClient(new Handler(message)));
        var rows = new List<Dictionary<string, object>>();
        await client.ExecuteStreamingQueryAsync("select", row => { rows.Add(row); return Task.CompletedTask; });
        Assert.Single(rows);
        Assert.Equal(1, Convert.ToInt32(rows[0]["ID"]));
    }

    [Fact]
    public async Task ExecuteStreamingQueryAsync_HttpError_Throws()
    {
        var message = new HttpResponseMessage(HttpStatusCode.BadRequest) { Content = new StringContent("err") };
        var client = new KsqlDbRestApiClient("http://unit", new HttpClient(new Handler(message)));
        await Assert.ThrowsAsync<KsqlDbException>(() => client.ExecuteStreamingQueryAsync("select", _ => Task.CompletedTask));
    }

    [Fact]
    public async Task ExecuteStreamingQueryAsync_Cancelled_Throws()
    {
        var message = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(string.Empty) };
        var client = new KsqlDbRestApiClient("http://unit", new HttpClient(new Handler(message)));
        var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAsync<KsqlDbException>(() => client.ExecuteStreamingQueryAsync("select", _ => Task.CompletedTask, cts.Token));
    }
}
