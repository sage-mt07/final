using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Linq;

internal class ReplaceParameterVisitor : ExpressionVisitor
{
    private readonly ParameterExpression _oldParameter;
    private readonly Expression _newExpression;

    private ReplaceParameterVisitor(ParameterExpression oldParameter, Expression newExpression)
    {
        _oldParameter = oldParameter;
        _newExpression = newExpression;
    }

    public static Expression Replace(Expression expression, ParameterExpression oldParameter, Expression newExpression)
    {
        return new ReplaceParameterVisitor(oldParameter, newExpression).Visit(expression);
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        return node == _oldParameter ? _newExpression : base.VisitParameter(node);
    }
}
