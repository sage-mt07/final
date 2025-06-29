using System;

namespace Kafka.Ksql.Linq.Serialization.Avro.Management;
public class SchemaUpgradeResult
{
    public bool Success { get; set; }
    public int? NewSchemaId { get; set; }
    public string Reason { get; set; } = string.Empty;
    public DateTime UpgradedAt { get; set; } = DateTime.UtcNow;
}
