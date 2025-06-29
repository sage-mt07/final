namespace Kafka.Ksql.Linq.Query.Ksql;

internal class KsqlStatementResponse
{
    public string StatementText { get; set; } = string.Empty;
    public string? CommandId { get; set; }
}
