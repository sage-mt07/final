using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Query.Pipeline;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.KsqlDslTests.Window;

public class MultiWindowAccessTests
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
    public void Window_WithDifferentDurations_GeneratesDistinctQueries()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr1 = src.Window(TimeSpan.FromMinutes(1)).Where(e => e.Id > 0);
        var expr5 = src.Window(TimeSpan.FromMinutes(5)).Where(e => e.Id > 0);

        var pipeline = CreatePipeline();
        var ksql1 = pipeline.GenerateKsqlQuery("Trades", expr1.Expression, false);
        var ksql5 = pipeline.GenerateKsqlQuery("Trades", expr5.Expression, false);

        Assert.NotEqual(ksql1, ksql5);
        Assert.Contains("trades_1min_window", ksql1);
        Assert.Contains("trades_5min_window", ksql5);
    }
}
