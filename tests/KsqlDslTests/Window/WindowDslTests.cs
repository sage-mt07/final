using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Pipeline;
using Kafka.Ksql.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.KsqlDslTests.Window;

public class WindowDslTests
{
    private class FakeExecutor : KsqlDbExecutor
    {
        public FakeExecutor() : base(new NullLoggerFactory()) { }
        public override void ExecuteDDL(string ddlQuery) { }
        public override System.Threading.Tasks.Task ExecuteDDLAsync(string ddlQuery) => System.Threading.Tasks.Task.CompletedTask;
        public override System.Threading.Tasks.Task<List<T>> ExecutePullQueryAsync<T>(string query) where T : class => System.Threading.Tasks.Task.FromResult(new List<T>());
        public override System.Threading.Tasks.Task<List<T>> ExecutePushQueryAsync<T>(string query) where T : class => System.Threading.Tasks.Task.FromResult(new List<T>());
        public override System.Threading.Tasks.Task StopAllQueriesAsync() => System.Threading.Tasks.Task.CompletedTask;
        public override void Dispose() { }
    }

    private static QueryExecutionPipeline CreatePipeline()
    {
        var executor = new FakeExecutor();
        var manager = new DerivedObjectManager(executor, new DDLQueryGenerator(new NullLoggerFactory()), new StreamTableAnalyzer(new NullLoggerFactory()), new NullLoggerFactory());
        var ddl = new DDLQueryGenerator(new NullLoggerFactory());
        var dml = new DMLQueryGenerator(new NullLoggerFactory());
        var analyzer = new StreamTableAnalyzer(new NullLoggerFactory());
        return new QueryExecutionPipeline(manager, ddl, dml, executor, analyzer, new NullLoggerFactory());
    }

    [Fact]
    public void GenerateCreateTableAs_WithWindowAndGroupBy()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.Window(TumblingWindow.OfMinutes(5))
                      .GroupBy(e => e.Type)
                      .Select(g => new { g.Key, Count = g.Count() });

        var generator = new DDLQueryGenerator(new NullLoggerFactory());
        var query = generator.GenerateCreateTableAs("t1", "Base", expr.Expression);

        Assert.Contains("WINDOW TUMBLING (SIZE 5 MINUTES)", query);
        Assert.Contains("GROUP BY Type", query);
    }

    [Fact]
    public void GenerateKsqlQuery_WindowGroupBySelect_PushQuery()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.Window(TumblingWindow.OfMinutes(5))
                      .GroupBy(e => e.Type)
                      .Select(g => new { g.Key });

        var pipeline = CreatePipeline();
        var ksql = pipeline.GenerateKsqlQuery("Base", expr.Expression, false);
        Assert.Contains("EMIT CHANGES", ksql);
    }
}
