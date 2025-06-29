using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Messaging.Configuration;
using Kafka.Ksql.Linq.Infrastructure.Admin;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Infrastructure;

public class KafkaAdminServiceTests
{
    private static KafkaAdminService CreateUninitialized(KsqlDslOptions options)
    {
        var svc = (KafkaAdminService)RuntimeHelpers.GetUninitializedObject(typeof(KafkaAdminService));
        typeof(KafkaAdminService)
            .GetField("_options", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(svc, options);
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
}
