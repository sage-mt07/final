using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Pipeline;
using Kafka.Ksql.Linq.Query.Schema;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Schema;

public class SchemaRegistryTests
{
    private class FakeExecutor : KsqlDbExecutor
    {
        public List<string> DdlQueries { get; } = new();
        public FakeExecutor() : base(new NullLoggerFactory()) { }
        public override void ExecuteDDL(string ddlQuery) => DdlQueries.Add(ddlQuery);
        public override Task ExecuteDDLAsync(string ddlQuery) { DdlQueries.Add(ddlQuery); return Task.CompletedTask; }
        public override Task<List<T>> ExecutePullQueryAsync<T>(string query) => Task.FromResult(new List<T>());
        public override Task<List<T>> ExecutePushQueryAsync<T>(string query) => Task.FromResult(new List<T>());
        public override Task StopAllQueriesAsync() => Task.CompletedTask;
        public override void Dispose() { }
    }

    [KsqlStream]
    private class StreamEntity
    {
        public int Id { get; set; }
    }

    [KsqlTable]
    private class TableEntity
    {
        public int Id { get; set; }
    }

    [Fact]
    public async Task RegisterSchemaAsync_UsesCreateTableWhenKeysPresent()
    {
        var executor = new FakeExecutor();
        var ddl = new DDLQueryGenerator(new NullLoggerFactory());
        var registry = new SchemaRegistry(executor, ddl, new NullLoggerFactory());
        var model = new EntityModel
        {
            EntityType = typeof(TestEntity),
            KeyProperties = new[] { typeof(TestEntity).GetProperty(nameof(TestEntity.Id))! },
            AllProperties = typeof(TestEntity).GetProperties()
        };
        await registry.RegisterSchemaAsync<TestEntity>(model);
        Assert.Contains("CREATE TABLE", executor.DdlQueries.First());
        Assert.True(registry.IsRegistered(typeof(TestEntity)));
    }

    [Fact]
    public async Task RegisterSchemaAsync_StreamAttribute_UsesCreateStream()
    {
        var executor = new FakeExecutor();
        var ddl = new DDLQueryGenerator(new NullLoggerFactory());
        var registry = new SchemaRegistry(executor, ddl, new NullLoggerFactory());
        var model = new EntityModel
        {
            EntityType = typeof(StreamEntity),
            AllProperties = typeof(StreamEntity).GetProperties()
        };
        await registry.RegisterSchemaAsync<StreamEntity>(model);
        Assert.Contains("CREATE STREAM", executor.DdlQueries.First());
        Assert.True(registry.IsRegistered(typeof(StreamEntity)));
    }

    [Fact]
    public async Task RegisterSchemaAsync_TableAttribute_UsesCreateTable()
    {
        var executor = new FakeExecutor();
        var ddl = new DDLQueryGenerator(new NullLoggerFactory());
        var registry = new SchemaRegistry(executor, ddl, new NullLoggerFactory());
        var model = new EntityModel
        {
            EntityType = typeof(TableEntity),
            AllProperties = typeof(TableEntity).GetProperties()
        };
        await registry.RegisterSchemaAsync<TableEntity>(model);
        Assert.Contains("CREATE TABLE", executor.DdlQueries.First());
        Assert.True(registry.IsRegistered(typeof(TableEntity)));
    }
    [Fact]
    public async Task UnregisterSchemaAsync_RemovesRegistration()
    {
        var executor = new FakeExecutor();
        var ddl = new DDLQueryGenerator(new NullLoggerFactory());
        var registry = new SchemaRegistry(executor, ddl, new NullLoggerFactory());
        var model = new EntityModel { EntityType = typeof(TestEntity), AllProperties = typeof(TestEntity).GetProperties() };
        await registry.RegisterSchemaAsync<TestEntity>(model);
        await registry.UnregisterSchemaAsync<TestEntity>();
        Assert.False(registry.IsRegistered(typeof(TestEntity)));
        Assert.Equal(2, executor.DdlQueries.Count);
    }

    [Fact]
    public async Task UnregisterAllSchemasAsync_RemovesAll()
    {
        var executor = new FakeExecutor();
        var ddl = new DDLQueryGenerator(new NullLoggerFactory());
        var registry = new SchemaRegistry(executor, ddl, new NullLoggerFactory());
        var model = new EntityModel { EntityType = typeof(TestEntity), AllProperties = typeof(TestEntity).GetProperties() };
        await registry.RegisterSchemaAsync<TestEntity>(model);
        await registry.RegisterSchemaAsync<StreamEntity>(new EntityModel { EntityType = typeof(StreamEntity), AllProperties = typeof(StreamEntity).GetProperties() });
        await registry.UnregisterAllSchemasAsync();
        Assert.False(registry.IsRegistered(typeof(TestEntity)));
        Assert.False(registry.IsRegistered(typeof(StreamEntity)));
        Assert.Equal(4, executor.DdlQueries.Count);
    }

    [Fact]
    public async Task Dispose_CallsCleanup()
    {
        var executor = new FakeExecutor();
        var ddl = new DDLQueryGenerator(new NullLoggerFactory());
        var registry = new SchemaRegistry(executor, ddl, new NullLoggerFactory());
        var model = new EntityModel { EntityType = typeof(TestEntity), AllProperties = typeof(TestEntity).GetProperties() };
        await registry.RegisterSchemaAsync<TestEntity>(model);
        registry.Dispose();
        Assert.Empty(registry.GetType().GetField("_registeredSchemas", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(registry) as System.Collections.IDictionary);
    }

    [Fact]
    public async Task GetObjectName_ReturnsName()
    {
        var executor = new FakeExecutor();
        var ddl = new DDLQueryGenerator(new NullLoggerFactory());
        var registry = new SchemaRegistry(executor, ddl, new NullLoggerFactory());
        var model = new EntityModel { EntityType = typeof(TestEntity), AllProperties = typeof(TestEntity).GetProperties() };
        await registry.RegisterSchemaAsync<TestEntity>(model);
        var name = registry.GetObjectName<TestEntity>();
        Assert.Equal("testentity_base", name);
    }
}
