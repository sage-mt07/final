using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Messaging.Abstractions;
using Kafka.Ksql.Linq.Messaging.Configuration;
using Kafka.Ksql.Linq.Messaging.Producers;
using Kafka.Ksql.Linq.Messaging.Producers.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;
using Kafka.Ksql.Linq.Tests.Serialization;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Messaging;
#nullable enable

public class KafkaProducerManagerDisposeTests
{
    private class Sample { }

    private class StubProducer<T> : IKafkaProducer<T> where T : class
    {
        public bool Disposed;
        public bool Sent;
        public string TopicName => "t";
        public Task<KafkaDeliveryResult> SendAsync(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
        {
            Sent = true;
            return Task.FromResult(new KafkaDeliveryResult());
        }
        public Task<KafkaBatchDeliveryResult> SendBatchAsync(IEnumerable<T> messages, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)
        {
            Sent = true;
            return Task.FromResult(new KafkaBatchDeliveryResult());
        }
        public Task FlushAsync(TimeSpan timeout) => Task.CompletedTask;
        public void Dispose() { Disposed = true; }
    }

    private class StubDisposable : IDisposable
    {
        public bool Disposed;
        public void Dispose() { Disposed = true; }
    }


    private static ConcurrentDictionary<Type, object> GetProducerDict(KafkaProducerManager manager)
        => (ConcurrentDictionary<Type, object>)typeof(KafkaProducerManager)
            .GetField("_producers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;

    private static ConcurrentDictionary<(Type, string), object> GetTopicProducerDict(KafkaProducerManager manager)
        => (ConcurrentDictionary<(Type, string), object>)typeof(KafkaProducerManager)
            .GetField("_topicProducers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;

    private static ConcurrentDictionary<Type, object> GetSerializationDict(KafkaProducerManager manager)
        => (ConcurrentDictionary<Type, object>)typeof(KafkaProducerManager)
            .GetField("_serializationManagers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;

    private static Lazy<Confluent.SchemaRegistry.ISchemaRegistryClient> GetSchemaLazy(KafkaProducerManager manager)
        => (Lazy<Confluent.SchemaRegistry.ISchemaRegistryClient>)typeof(KafkaProducerManager)
            .GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;

    private static bool GetDisposedFlag(KafkaProducerManager manager)
        => (bool)typeof(KafkaProducerManager)
            .GetField("_disposed", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(manager)!;

    [Fact]
    public void Dispose_BeforeProducerCreation_DoesNotThrow()
    {
        var manager = new KafkaProducerManager(Options.Create(new KsqlDslOptions()), new NullLoggerFactory());
        manager.Dispose();

        Assert.True(GetDisposedFlag(manager));
        Assert.Empty(GetProducerDict(manager));
        Assert.Empty(GetTopicProducerDict(manager));
        Assert.Empty(GetSerializationDict(manager));
        Assert.False(GetSchemaLazy(manager).IsValueCreated);
    }

    [Fact]
    public void Dispose_WithCachedResources_DisposesAll()
    {
        var manager = new KafkaProducerManager(Options.Create(new KsqlDslOptions()), new NullLoggerFactory());
        var producers = GetProducerDict(manager);
        var topics = GetTopicProducerDict(manager);
        var serials = GetSerializationDict(manager);
        var schemaLazyField = typeof(KafkaProducerManager).GetField("_schemaRegistryClient", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var proxy = DispatchProxy.Create<ISchemaRegistryClient, FakeSchemaRegistryClient>();
        var schemaStub = (FakeSchemaRegistryClient)proxy!;
        schemaLazyField.SetValue(manager, new Lazy<ISchemaRegistryClient>(() => proxy));

        var p1 = new StubProducer<Sample>();
        var p2 = new StubProducer<Sample>();
        producers[typeof(Sample)] = p1;
        topics[(typeof(Sample), "t")] = p2;
        var ser = new StubDisposable();
        serials[typeof(Sample)] = ser;

        manager.Dispose();

        Assert.True(p1.Disposed);
        Assert.True(p2.Disposed);
        Assert.True(ser.Disposed);
        Assert.False(schemaStub.Disposed);
        Assert.Empty(producers);
        Assert.Empty(topics);
        Assert.Empty(serials);
        Assert.True(GetDisposedFlag(manager));
    }

    [Fact]
    public async Task Dispose_AfterUse_DisposesProducers()
    {
        var manager = new KafkaProducerManager(Options.Create(new KsqlDslOptions()), new NullLoggerFactory());
        var producers = GetProducerDict(manager);
        var stub = new StubProducer<Sample>();
        producers[typeof(Sample)] = stub;

        await manager.SendAsync(new Sample());
        Assert.True(stub.Sent);

        manager.Dispose();
        Assert.True(stub.Disposed);
        Assert.True(GetDisposedFlag(manager));
    }
}
