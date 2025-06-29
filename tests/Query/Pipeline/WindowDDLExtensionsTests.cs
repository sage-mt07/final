using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Pipeline;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class WindowDDLExtensionsTests
{
    private class Sample
    {
        public bool Flag { get; set; }
        public DateTime Created { get; set; }
    }

    [Theory]
    [InlineData(nameof(Sample.Flag), "boolean")]
    [InlineData(nameof(Sample.Created), "long")]
    public void MapPropertyTypeToAvroType_ReturnsExpected(string propName, string expected)
    {
        var prop = typeof(Sample).GetProperty(propName)!;
        var result = InvokePrivate<string>(typeof(WindowDDLExtensions), "MapPropertyTypeToAvroType", new[] { typeof(PropertyInfo) }, null, prop);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GenerateAggregationFunction_ReturnsWindowFunctions()
    {
        var visitor = new WindowSelectExpressionVisitor();
        var param = Expression.Parameter(typeof(IGrouping<int, TestEntity>), "g");
        var startCall = Expression.Call(typeof(WindowInfoExtensions).GetMethod("WindowStart")!.MakeGenericMethod(typeof(TestEntity), typeof(int)), param);
        var result = InvokePrivate<string>(visitor, "GenerateAggregationFunction", new[] { typeof(MethodCallExpression) }, null, startCall);
        Assert.Equal("WINDOWSTART", result);

        var lambdaParam = Expression.Parameter(typeof(TestEntity), "x");
        var lambda = Expression.Lambda(Expression.Property(lambdaParam, nameof(TestEntity.Id)), lambdaParam);
        var sumMethod = typeof(Enumerable).GetMethods().First(m => m.Name == "Sum" && m.GetParameters().Length == 2).MakeGenericMethod(typeof(TestEntity));
        var sumCall = Expression.Call(sumMethod, param, lambda);
        result = InvokePrivate<string>(visitor, "GenerateAggregationFunction", new[] { typeof(MethodCallExpression) }, null, sumCall);
        Assert.Equal("SUM(ID)", result);

        var countMethod = typeof(Enumerable).GetMethods().First(m => m.Name == "Count" && m.GetParameters().Length == 1).MakeGenericMethod(typeof(TestEntity));
        var countCall = Expression.Call(countMethod, param);
        result = InvokePrivate<string>(visitor, "GenerateAggregationFunction", new[] { typeof(MethodCallExpression) }, null, countCall);
        Assert.Equal("COUNT(*)", result);
    }

    private class TimedEntity
    {
        public int Id { get; set; }
        [AvroTimestamp]
        public DateTime Created { get; set; }
    }

    [Fact]
    public void GenerateCreateWindowTable_BuildsQuery()
    {
        var generator = new DDLQueryGenerator(new NullLoggerFactory());

        Expression<Func<IGrouping<int, TimedEntity>, object>> agg = g => new { Count = g.Count() };
        Expression<Func<TimedEntity, object>> groupBy = e => e.Id;

        var sql = generator.GenerateCreateWindowTable(
            "win_table",
            "source",
            10,
            groupBy.Body,
            agg.Body,
            TimeSpan.FromSeconds(5));

        Assert.Contains("CREATE TABLE win_table AS", sql);
        Assert.Contains("FROM source", sql);
        Assert.Contains("WINDOW TUMBLING (SIZE 10 MINUTES, GRACE PERIOD 5", sql);
        Assert.Contains("GROUP BY ID", sql);
        Assert.Contains("COUNT(*) AS Count", sql);
    }

    [Fact]
    public void GenerateWindowAvroSchema_IncludesWindowFields()
    {
        var generator = new DDLQueryGenerator(new NullLoggerFactory());
        var model = new EntityModel
        {
            EntityType = typeof(TimedEntity),
            AllProperties = typeof(TimedEntity).GetProperties(),
            KeyProperties = new[] { typeof(TimedEntity).GetProperty(nameof(TimedEntity.Id))! },
            TopicAttribute = new TopicAttribute("src")
        };

        var schema = generator.GenerateWindowAvroSchema(model, "win_table");

        Assert.Contains("\"name\": \"win_table\"", schema);
        Assert.Contains("WINDOWSTART", schema);
        Assert.Contains("WINDOWEND", schema);
    }
}
