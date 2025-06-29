using System;

namespace Kafka.Ksql.Linq.Core.Abstractions;
[AttributeUsage(AttributeTargets.Class)]
public class KsqlTableAttribute : Attribute
{
    public string? TableName { get; set; }
    public int? Partitions { get; set; }
}
