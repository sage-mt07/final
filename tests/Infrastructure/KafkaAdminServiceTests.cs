using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Messaging.Configuration;
using Kafka.Ksql.Linq.Infrastructure.Admin;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Infrastructure;

public class KafkaAdminServiceTests
{
    private class FakeAdminClient : DispatchProxy
    {
        public Func<TopicCollection, DescribeTopicsOptions?, CancellationToken, Task<List<TopicMetadata>>> DescribeHandler { get; set; } = (_, __, ___) => Task.FromResult(new List<TopicMetadata>());
        public Func<IEnumerable<TopicSpecification>, CreateTopicsOptions?, Task> CreateHandler { get; set; } = (_, __) => Task.CompletedTask;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod?.Name)
            {
                case nameof(IAdminClient.CreateTopicsAsync):
                    return CreateHandler((IEnumerable<TopicSpecification>)args![0]!, (CreateTopicsOptions?)args[1]);
                case nameof(IAdminClient.DescribeTopicsAsync):
                    return DescribeHandler(
                        (TopicCollection)args![0]!,
                        (DescribeTopicsOptions?)args[1]!,
                        (CancellationToken)args[2]!);
                case "Dispose":
                    return null;
                case "get_Name":
                    return "fake";
                case "get_Handle":
                    return null!;
            }
            throw new NotImplementedException(targetMethod?.Name);
        }
    }
    private static KafkaAdminService CreateUninitialized(KsqlDslOptions options, IAdminClient? adminClient = null)
    {
        var svc = (KafkaAdminService)RuntimeHelpers.GetUninitializedObject(typeof(KafkaAdminService));
        typeof(KafkaAdminService)
            .GetField("_options", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(svc, options);
        if (adminClient != null)
        {
            typeof(KafkaAdminService)
                .GetField("_adminClient", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(svc, adminClient);
        }
        return svc;
    }

    [Fact]
    public void CreateAdminConfig_Plaintext_ReturnsBasicSettings()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection
            {
                BootstrapServers = "server:9092",
                ClientId = "cid",
                AdditionalProperties = new Dictionary<string, string> { ["p"] = "v" }
            }
        };

        var svc = CreateUninitialized(options);
        var config = InvokePrivate<AdminClientConfig>(svc, "CreateAdminConfig", Type.EmptyTypes);

        Assert.Equal("server:9092", config.BootstrapServers);
        Assert.Equal("cid-admin", config.ClientId);
        Assert.Equal(options.Common.MetadataMaxAgeMs, config.MetadataMaxAgeMs);
        Assert.Equal("v", config.Get("p"));
    }

    [Fact]
    public void CreateAdminConfig_WithSecurityOptions()
    {
        var options = new KsqlDslOptions
        {
            Common = new CommonSection
            {
                BootstrapServers = "server:9092",
                ClientId = "cid",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "user",
                SaslPassword = "pass",
                SslCaLocation = "ca.crt",
                SslCertificateLocation = "cert.crt",
                SslKeyLocation = "key.key",
                SslKeyPassword = "pw"
            }
        };

        var svc = CreateUninitialized(options);
        var config = InvokePrivate<AdminClientConfig>(svc, "CreateAdminConfig", Type.EmptyTypes);

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.Plain, config.SaslMechanism);
        Assert.Equal("user", config.SaslUsername);
        Assert.Equal("pass", config.SaslPassword);
        Assert.Equal("ca.crt", config.SslCaLocation);
        Assert.Equal("cert.crt", config.SslCertificateLocation);
        Assert.Equal("key.key", config.SslKeyLocation);
        Assert.Equal("pw", config.SslKeyPassword);
    }

    [Fact]
    public async Task CreateDbTopicAsync_Succeeds_WhenTopicDoesNotExist()
    {
        var options = new KsqlDslOptions();
        var proxy = DispatchProxy.Create<IAdminClient, FakeAdminClient>();
        var fake = (FakeAdminClient)proxy!;
        var created = false;
        fake.DescribeHandler = (_, __, ___) => Task.FromResult(new List<TopicMetadata>());
        fake.CreateHandler = (_, __) => { created = true; return Task.CompletedTask; };

        var svc = CreateUninitialized(options, proxy);
        await svc.CreateDbTopicAsync("t", 1, 1);

        Assert.True(created);
    }

    [Fact]
    public async Task CreateDbTopicAsync_NoOp_WhenTopicExists()
    {
        var options = new KsqlDslOptions();
        var proxy = DispatchProxy.Create<IAdminClient, FakeAdminClient>();
        var fake = (FakeAdminClient)proxy!;
        var meta = (TopicMetadata)RuntimeHelpers.GetUninitializedObject(typeof(TopicMetadata));
        fake.DescribeHandler = (_, __, ___) => Task.FromResult(new List<TopicMetadata> { meta });
        var created = false;
        fake.CreateHandler = (_, __) => { created = true; return Task.CompletedTask; };

        var svc = CreateUninitialized(options, proxy);
        await svc.CreateDbTopicAsync("t", 1, 1);

        Assert.False(created);
    }

    [Fact]
    public async Task CreateDbTopicAsync_Throws_WhenKafkaError()
    {
        var options = new KsqlDslOptions();
        var proxy = DispatchProxy.Create<IAdminClient, FakeAdminClient>();
        var fake = (FakeAdminClient)proxy!;
        fake.DescribeHandler = (_, __, ___) => Task.FromResult(new List<TopicMetadata>());
        fake.CreateHandler = (_, __) => throw new KafkaException(new Error(ErrorCode.Local_Transport));

        var svc = CreateUninitialized(options, proxy);

        await Assert.ThrowsAsync<KafkaException>(() => svc.CreateDbTopicAsync("t", 1, 1));
    }

    [Theory]
    [InlineData(null, 1, (short)1)]
    [InlineData("", 1, (short)1)]
    [InlineData("t", 0, (short)1)]
    [InlineData("t", 1, (short)0)]
    public async Task CreateDbTopicAsync_InvalidParameters_Throws(string name, int partitions, short rep)
    {
        var options = new KsqlDslOptions();
        var proxy = DispatchProxy.Create<IAdminClient, FakeAdminClient>();
        var svc = CreateUninitialized(options, proxy);

        await Assert.ThrowsAsync<ArgumentException>(() => svc.CreateDbTopicAsync(name!, partitions, rep));
    }
}
