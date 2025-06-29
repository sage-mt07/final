using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

internal class OperationInfo
{
    public string MethodName { get; set; } = string.Empty;
    public MethodCallExpression Expression { get; set; } = null!;
    public OperationCharacteristics Characteristics { get; set; } = null!;
    public int Order { get; set; }
}
