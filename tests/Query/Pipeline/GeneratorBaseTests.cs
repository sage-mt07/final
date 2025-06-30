using System;
using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class GeneratorBaseTests
{
    [Fact]
    public void AssembleQuery_OrdersPartsAndTrims()
    {
        var select = QueryPart.Required("SELECT *", 10);
        var from = QueryPart.Required("FROM t", 20);
        var where = QueryPart.Required("WHERE Id = 1", 40);

        var result = PrivateAccessor.InvokePrivate<string>(
            typeof(GeneratorBase),
            "AssembleQuery",
            new[] { typeof(QueryPart[]) },
            args: new object[] { new[] { where, select, from } });

        Assert.Equal("SELECT * FROM t WHERE Id = 1", result);
    }

    [Fact]
    public void AssembleQuery_IgnoresEmptyOptionalParts()
    {
        var select = QueryPart.Required("SELECT *", 10);
        var emptyOpt = QueryPart.Optional(string.Empty, 30);
        var from = QueryPart.Required("FROM t", 20);

        var result = PrivateAccessor.InvokePrivate<string>(
            typeof(GeneratorBase),
            "AssembleQuery",
            new[] { typeof(QueryPart[]) },
            args: new object[] { new[] { select, emptyOpt, from } });

        Assert.Equal("SELECT * FROM t", result);
    }

    [Fact]
    public void AssembleQuery_NoValidParts_Throws()
    {
        Assert.ThrowsAny<Exception>(() =>
            PrivateAccessor.InvokePrivate<string>(
                typeof(GeneratorBase),
                "AssembleQuery",
                new[] { typeof(QueryPart[]) },
                args: new object[] { new[] { QueryPart.Optional(string.Empty) } }));
    }
}
