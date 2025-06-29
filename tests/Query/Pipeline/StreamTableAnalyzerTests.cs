using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Kafka.Ksql.Linq.Query.Pipeline;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class StreamTableAnalyzerTests
{
    [Fact]
    public void Analyze_SimpleWhereSelect_ReturnsStreamOutput()
    {
        IQueryable<TestEntity> source = new List<TestEntity>().AsQueryable();
        var query = source.Where(e => e.IsActive).Select(e => new { e.Id });
        var analyzer = new StreamTableAnalyzer(new NullLoggerFactory());

        var result = analyzer.AnalyzeExpression(query.Expression);

        Assert.False(result.HasGroupBy);
        Assert.False(result.HasAggregation);
        Assert.True(result.RequiresStreamOutput);
        Assert.False(result.RequiresTableOutput);
        Assert.Equal(new[] {"Where", "Select"}, result.OperationChain.Select(o => o.MethodName));
    }

    [Fact]
    public void Analyze_GroupBySelect_ReturnsTableOutput()
    {
        IQueryable<TestEntity> source = new List<TestEntity>().AsQueryable();
        var query = source.GroupBy(e => e.Type).Select(g => new { g.Key, Count = g.Count() });
        var analyzer = new StreamTableAnalyzer(new NullLoggerFactory());

        var result = analyzer.AnalyzeExpression(query.Expression);

        Assert.True(result.HasGroupBy);
        Assert.True(result.HasAggregation);
        Assert.False(result.RequiresStreamOutput);
        Assert.True(result.RequiresTableOutput);
    }
}
