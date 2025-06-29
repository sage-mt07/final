using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;

namespace Kafka.Ksql.Linq.Tests.Serialization;

#nullable enable

internal class FakeSchemaRegistryClient : DispatchProxy
{
    public bool Disposed { get; private set; }
    public List<string> RegisterSubjects { get; } = new();
    public int RegisterReturn { get; set; } = 1;
    public bool CompatibilityResult { get; set; } = true;
    public List<int> VersionsResult { get; set; } = new();
    public int LatestVersion { get; set; } = 1;
    public string SchemaString { get; set; } = "schema";

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        switch (targetMethod?.Name)
        {
            case nameof(ISchemaRegistryClient.RegisterSchemaAsync):
                RegisterSubjects.Add((string)args![0]!);
                return Task.FromResult(RegisterReturn);
            case nameof(ISchemaRegistryClient.IsCompatibleAsync):
                return Task.FromResult(CompatibilityResult);
            case nameof(ISchemaRegistryClient.GetSubjectVersionsAsync):
                return Task.FromResult(VersionsResult);
            case nameof(ISchemaRegistryClient.GetRegisteredSchemaAsync):
                var subject = (string)args![0]!;
                var version = (int)args[1]!;
                var regType = typeof(Schema).Assembly.GetType("Confluent.SchemaRegistry.RegisteredSchema")!;
                var schemaVersion = version == -1 ? LatestVersion : version;
                var obj = Activator.CreateInstance(regType, subject, schemaVersion, RegisterReturn, SchemaString, SchemaType.Avro, null)!;
                var fromResult = typeof(Task)
                    .GetMethod("FromResult")!
                    .MakeGenericMethod(regType)
                    .Invoke(null, new[] { obj });
                return fromResult!;
            case nameof(IDisposable.Dispose):
                Disposed = true;
                return null;
        }
        throw new NotImplementedException(targetMethod?.Name);
    }
}
