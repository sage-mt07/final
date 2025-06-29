using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Query.Pipeline;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Pipeline;

public class QueryExecutionPipelineTests
{
    private class FakeExecutor : KsqlDbExecutor
    {
        public bool StopCalled { get; private set; }
        public bool Disposed { get; private set; }
        public FakeExecutor() : base(new NullLoggerFactory()) { }
        public override void ExecuteDDL(string ddlQuery) { }
        public override Task ExecuteDDLAsync(string ddlQuery) => Task.CompletedTask;
        public override Task<List<T>> ExecutePullQueryAsync<T>(string query) where T : class => Task.FromResult(new List<T>());
        public override Task<List<T>> ExecutePushQueryAsync<T>(string query) where T : class => Task.FromResult(new List<T>());
        public override Task StopAllQueriesAsync() { StopCalled = true; return Task.CompletedTask; }
        public override void Dispose() { Disposed = true; }
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
    public void GenerateKsqlQuery_WhereSelect_PullQuery()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.Where(e => e.IsActive).Select(e => new { e.Id }).Expression;
        var pipeline = CreatePipeline();

        var ksql = pipeline.GenerateKsqlQuery("Base", expr, true);

        Assert.Matches("^SELECT \\* FROM Base_stream_\\d+_stream_\\d+$", ksql);
    }

    [Fact]
    public void GenerateKsqlQuery_GroupBySelect_PushQuery()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.GroupBy(e => e.Type).Select(g => new { g.Key, Count = g.Count() }).Expression;
        var pipeline = CreatePipeline();

        var ksql = pipeline.GenerateKsqlQuery("Base", expr, false);

        Assert.Matches("^SELECT \\* FROM Base_table_\\d+_table_\\d+ EMIT CHANGES$", ksql);
    }

    [Fact]
    public async Task StopAllStreamingQueriesAsync_CallsExecutor()
    {
        var executor = new FakeExecutor();
        var manager = new DerivedObjectManager(executor, new DDLQueryGenerator(new NullLoggerFactory()), new StreamTableAnalyzer(new NullLoggerFactory()), new NullLoggerFactory());
        var pipeline = new QueryExecutionPipeline(manager, new DDLQueryGenerator(new NullLoggerFactory()), new DMLQueryGenerator(new NullLoggerFactory()), executor, new StreamTableAnalyzer(new NullLoggerFactory()), new NullLoggerFactory());

        await pipeline.StopAllStreamingQueriesAsync();

        Assert.True(executor.StopCalled);
    }
    [Fact]
    public void GetDiagnostics_ReturnsConstant()
    {
        var pipeline = CreatePipeline();
        Assert.Equal("Diagnostics removed in Phase1", pipeline.GetDiagnostics());
    }

    [Fact]
    public void ToKsql_CallsGenerateKsqlQuery()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.Where(e => e.IsActive).Expression;
        var pipeline = CreatePipeline();
        var sql = pipeline.ToKsql(expr, "Base", false);
        Assert.Contains("FROM", sql);
    }

    [Fact]
    public void IsPullQuery_ReflectsDiagnosticsMetadata()
    {
        var pipeline = CreatePipeline();
        var diagField = typeof(QueryExecutionPipeline).GetField("_diagnostics", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var diag = (QueryDiagnostics)diagField.GetValue(pipeline)!;
        diag.SetMetadata("IsPullQuery", true);
        Assert.True(pipeline.IsPullQuery());
    }

    [Fact]
    public void Dispose_CleansUpAndDisposesExecutor()
    {
        var executor = new FakeExecutor();
        var manager = new DerivedObjectManager(executor, new DDLQueryGenerator(new NullLoggerFactory()), new StreamTableAnalyzer(new NullLoggerFactory()), new NullLoggerFactory());
        var pipeline = new QueryExecutionPipeline(manager, new DDLQueryGenerator(new NullLoggerFactory()), new DMLQueryGenerator(new NullLoggerFactory()), executor, new StreamTableAnalyzer(new NullLoggerFactory()), new NullLoggerFactory());
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        manager.CreateDerivedStream("Base", src.Expression);
        pipeline.Dispose();
        var dict = manager.GetType().GetField("_derivedObjects", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(manager) as System.Collections.IDictionary;
        Assert.Empty(dict!);
        Assert.True(executor.Disposed);
    }

    [Fact]
    public async Task ExecuteQueryAsync_ReturnsResult()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src.Where(e => e.IsActive).Expression;
        var pipeline = CreatePipeline();
        var result = await pipeline.ExecuteQueryAsync<TestEntity>("Base", expr, QueryExecutionMode.PullQuery);
        Assert.True(result.Success);
        Assert.NotEqual(default, result.ExecutedAt);
        Assert.NotNull(result.Data);
    }
}
