using System;

namespace Kafka.Ksql.Linq.Query.Pipeline;
internal class QueryExecutionResult
{
    public bool Success { get; set; }
    public string TargetObject { get; set; } = string.Empty;
    public object? Data { get; set; }
    public DateTime ExecutedAt { get; set; }
    public string? ErrorMessage { get; set; }
}
