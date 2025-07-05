using System;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration;

public class KsqlSyntaxTests
{
    private readonly IKsqlClient _client;

    public KsqlSyntaxTests()
    {
        _client = new KsqlClient(new Uri("http://localhost:8088"));
    }

    [Theory(Skip = "ksqlDB環境がないため、PR時は実行しない")]
    [Trait("Category", "Integration")]
    [InlineData("CREATE STREAM test_stream AS SELECT * FROM source EMIT CHANGES;")]
    [InlineData("SELECT CustomerId, COUNT(*) FROM orders GROUP BY CustomerId EMIT CHANGES;")]
    public async Task GeneratedQuery_IsValidInKsqlDb(string ksql)
    {
        var response = await _client.ExecuteExplainAsync(ksql);
        Assert.True(response.IsSuccess, $"{ksql} failed: {response.Message}");
    }
}
