using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;

namespace Kafka.Ksql.Linq.Query.Builders;

/// <summary>
/// WINDOW句内容構築ビルダー
/// 設計理由：責務分離設計に準拠、キーワード除外で純粋なウィンドウ定義内容のみ生成
/// 出力例: "TUMBLING (SIZE 5 MINUTES)" (WINDOW除外)
/// </summary>
internal class WindowClauseBuilder : BuilderBase
{
    public override KsqlBuilderType BuilderType => KsqlBuilderType.Window;

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return Array.Empty<KsqlBuilderType>(); // 他Builderに依存しない
    }

    protected override string BuildInternal(Expression expression)
    {
        var visitor = new WindowExpressionVisitor();
        
        // 式の種類に応じた処理
        switch (expression)
        {
            case ConstantExpression { Value: WindowDef def }:
                visitor.VisitWindowDef(def);
                break;
                
            case ConstantExpression { Value: TimeSpan ts }:
                visitor.VisitWindowDef(TumblingWindow.Of(ts));
                break;
                
            default:
                visitor.Visit(expression);
                break;
        }

        return visitor.BuildWindowClause();
    }

    protected override void ValidateBuilderSpecific(Expression expression)
    {
        // WINDOW句特有のバリデーション
        ValidateWindowExpression(expression);
    }

    /// <summary>
    /// ウィンドウ式バリデーション
    /// </summary>
    private static void ValidateWindowExpression(Expression expression)
    {
        // WindowDefまたはTimeSpanの確認
        if (expression is ConstantExpression constant)
        {
            if (constant.Value is WindowDef windowDef)
            {
                ValidateWindowDef(windowDef);
            }
            else if (constant.Value is TimeSpan timeSpan)
            {
                ValidateTimeSpan(timeSpan);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Window expression must be WindowDef or TimeSpan, but got {constant.Value?.GetType().Name}");
            }
        }
        else if (expression is MethodCallExpression methodCall)
        {
            ValidateWindowMethodCall(methodCall);
        }
        else
        {
            throw new InvalidOperationException(
                $"Unsupported window expression type: {expression.GetType().Name}");
        }
    }

    /// <summary>
    /// WindowDef バリデーション
    /// </summary>
    private static void ValidateWindowDef(WindowDef windowDef)
    {
        var operations = windowDef.Operations;
        
        // 基本的なウィンドウタイプの存在確認
        var hasWindowType = operations.Any(op => 
            op.Name == nameof(WindowDef.TumblingWindow) ||
            op.Name == nameof(WindowDef.HoppingWindow) ||
            op.Name == nameof(WindowDef.SessionWindow));
            
        if (!hasWindowType)
        {
            throw new InvalidOperationException("Window definition must specify window type (Tumbling, Hopping, or Session)");
        }

        // セッションウィンドウの場合はGapが必須
        if (operations.Any(op => op.Name == nameof(WindowDef.SessionWindow)))
        {
            if (!operations.Any(op => op.Name == nameof(WindowDef.Gap)))
            {
                throw new InvalidOperationException("Session window requires Gap specification");
            }
        }

        // HoppingウィンドウはSizeとAdvanceByが必要
        if (operations.Any(op => op.Name == nameof(WindowDef.HoppingWindow)))
        {
            if (!operations.Any(op => op.Name == nameof(WindowDef.Size)))
            {
                throw new InvalidOperationException("Hopping window requires Size specification");
            }
        }

        // 時間値の妥当性チェック
        ValidateWindowTiming(operations);
    }

    /// <summary>
    /// TimeSpan バリデーション
    /// </summary>
    private static void ValidateTimeSpan(TimeSpan timeSpan)
    {
        if (timeSpan <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("Window time span must be positive");
        }

        if (timeSpan.TotalDays > 30)
        {
            throw new InvalidOperationException(
                "Window time span should not exceed 30 days for performance reasons");
        }

        if (timeSpan.TotalSeconds < 1)
        {
            throw new InvalidOperationException(
                "Window time span should be at least 1 second");
        }
    }

    /// <summary>
    /// ウィンドウメソッド呼び出しバリデーション
    /// </summary>
    private static void ValidateWindowMethodCall(MethodCallExpression methodCall)
    {
        var validMethods = new[]
        {
            "TumblingWindow", "HoppingWindow", "SessionWindow",
            "Size", "AdvanceBy", "Gap", "Retention", "GracePeriod", "EmitFinal"
        };

        if (!validMethods.Contains(methodCall.Method.Name))
        {
            throw new InvalidOperationException(
                $"Method '{methodCall.Method.Name}' is not supported in window expressions");
        }
    }

    /// <summary>
    /// ウィンドウタイミングバリデーション
    /// </summary>
    private static void ValidateWindowTiming(List<(string Name, object? Value)> operations)
    {
        var sizeOp = operations.FirstOrDefault(op => op.Name == nameof(WindowDef.Size));
        var advanceByOp = operations.FirstOrDefault(op => op.Name == nameof(WindowDef.AdvanceBy));
        var gapOp = operations.FirstOrDefault(op => op.Name == nameof(WindowDef.Gap));

        // HoppingウィンドウでAdvanceBy > Sizeは非推奨
        if (sizeOp.Value is TimeSpan size && advanceByOp.Value is TimeSpan advanceBy)
        {
            if (advanceBy > size)
            {
                Console.WriteLine("[KSQL-LINQ WARNING] AdvanceBy greater than Size may cause gaps in data coverage");
            }
        }

        // セッションウィンドウのGapが大きすぎる場合の警告
        if (gapOp.Value is TimeSpan gap && gap.TotalHours > 24)
        {
            Console.WriteLine("[KSQL-LINQ WARNING] Session window gap over 24 hours may impact performance");
        }
    }
}

/// <summary>
/// WINDOW句専用ExpressionVisitor（リファクタリング版）
/// </summary>
internal class WindowExpressionVisitor : ExpressionVisitor
{
    private string _windowType = "";
    private string _size = "";
    private string _advanceBy = "";
    private string _gap = "";
    private string _retention = "";
    private string _gracePeriod = "";
    private string _emitBehavior = "";

    /// <summary>
    /// WindowDef訪問
    /// </summary>
    public void VisitWindowDef(WindowDef def)
    {
        foreach (var (Name, Value) in def.Operations)
        {
            ProcessWindowOperation(Name, Value);
        }
    }

    /// <summary>
    /// ウィンドウ操作処理
    /// </summary>
    private void ProcessWindowOperation(string name, object? value)
    {
        switch (name)
        {
            case nameof(WindowDef.TumblingWindow):
                _windowType = "TUMBLING";
                break;
                
            case nameof(WindowDef.HoppingWindow):
                _windowType = "HOPPING";
                break;
                
            case nameof(WindowDef.SessionWindow):
                _windowType = "SESSION";
                break;
                
            case nameof(WindowDef.Size):
                _size = FormatTimeSpan((TimeSpan)value!);
                break;
                
            case nameof(WindowDef.AdvanceBy):
                _advanceBy = FormatTimeSpan((TimeSpan)value!);
                break;
                
            case nameof(WindowDef.Gap):
                _gap = FormatTimeSpan((TimeSpan)value!);
                break;
                
            case nameof(WindowDef.Retention):
                _retention = FormatTimeSpan((TimeSpan)value!);
                break;
                
            case nameof(WindowDef.GracePeriod):
                _gracePeriod = FormatTimeSpan((TimeSpan)value!);
                break;
                
            case nameof(WindowDef.EmitFinal):
                _emitBehavior = "FINAL";
                break;
        }
    }

    /// <summary>
    /// ウィンドウ句構築
    /// </summary>
    public string BuildWindowClause()
    {
        return _windowType switch
        {
            "TUMBLING" => BuildTumblingClause(),
            "HOPPING" => BuildHoppingClause(),
            "SESSION" => BuildSessionClause(),
            _ => "UNKNOWN_WINDOW"
        };
    }

    /// <summary>
    /// TUMBLINGウィンドウ句構築
    /// </summary>
    private string BuildTumblingClause()
    {
        var clause = $"TUMBLING (SIZE {_size}";

        // オプション追加
        if (!string.IsNullOrEmpty(_retention))
            clause += $", RETENTION {_retention}";

        if (!string.IsNullOrEmpty(_gracePeriod))
            clause += $", GRACE PERIOD {_gracePeriod}";

        clause += ")";

        // EMIT句追加
        if (!string.IsNullOrEmpty(_emitBehavior))
            clause += $" EMIT {_emitBehavior}";

        return clause;
    }
    /// <summary>
    /// TimeSpanフォーマット
    /// </summary>
    private string FormatTimeSpan(TimeSpan timeSpan)
    {
        // 最適な単位で表現
        if (timeSpan.TotalDays >= 1 && timeSpan.TotalDays == Math.Floor(timeSpan.TotalDays))
            return $"{(int)timeSpan.TotalDays} DAYS";
        if (timeSpan.TotalHours >= 1 && timeSpan.TotalHours == Math.Floor(timeSpan.TotalHours))
            return $"{(int)timeSpan.TotalHours} HOURS";
        if (timeSpan.TotalMinutes >= 1 && timeSpan.TotalMinutes == Math.Floor(timeSpan.TotalMinutes))
            return $"{(int)timeSpan.TotalMinutes} MINUTES";
        if (timeSpan.TotalSeconds >= 1 && timeSpan.TotalSeconds == Math.Floor(timeSpan.TotalSeconds))
            return $"{(int)timeSpan.TotalSeconds} SECONDS";
        if (timeSpan.TotalMilliseconds >= 1)
            return $"{(int)timeSpan.TotalMilliseconds} MILLISECONDS";

        return "0 SECONDS";
    }
    /// <summary>
    /// HOPPINGウィンドウ句構築
    /// </summary>
    private string BuildHoppingClause()
    {
        var clause = $"HOPPING (SIZE {_size}";

        // ADVANCE BY は必須ではないが、指定されていれば追加
        if (!string.IsNullOrEmpty(_advanceBy))
            clause += $", ADVANCE BY {_advanceBy}";

        // オプション追加
        if (!string.IsNullOrEmpty(_retention))
            clause += $", RETENTION {_retention}";

        if (!string.IsNullOrEmpty(_gracePeriod))
            clause += $", GRACE PERIOD {_gracePeriod}";

        clause += ")";

        // EMIT句追加
        if (!string.IsNullOrEmpty(_emitBehavior))
            clause += $" EMIT {_emitBehavior}";

        return clause;
    }

    /// <summary>
    /// SESSIONウィンドウ句構築
    /// </summary>
    private string BuildSessionClause()
    {
        if (string.IsNullOrEmpty(_gap))
        {
            throw new InvalidOperationException("SESSION window requires GAP specification");
        }

        var clause = $"SESSION (GAP {_gap})";

        // セッションウィンドウはRETENTION、GRACE PERIOD、EMIT FINALをサポートしない
        // 警告表示
        if (!string.IsNullOrEmpty(_retention) || !string.IsNullOrEmpty(_gracePeriod) || !string.IsNullOrEmpty(_emitBehavior))
        {
            Console.WriteLine("[KSQL-LINQ WARNING] SESSION windows do not support RETENTION, GRACE PERIOD, or EMIT FINAL options");
        }

        return clause;
    }

    // TimeSpanフォーマット等のメソッドは省略（既に実装済み）
}

/// <summary>
/// Helper for processing LINQ Window method calls into KSQL WINDOW clauses.
/// </summary>
internal static class WindowExpressionVisitorExtensions
{
    internal static string ProcessWindowOperation(MethodCallExpression methodCall)
    {
        if (methodCall == null)
            throw new ArgumentNullException(nameof(methodCall));

        if (methodCall.Method.Name != "Window")
            throw new InvalidOperationException("Expression is not a Window call");

        if (methodCall.Arguments[0] is MethodCallExpression inner &&
            inner.Method.Name == "Window")
        {
            throw new InvalidOperationException("Multiple Window calls are not supported");
        }

        if (methodCall.Arguments.Count < 2)
            throw new InvalidOperationException("Window call missing definition");

        var argument = methodCall.Arguments[1];

        // evaluate the argument expression so WindowClauseBuilder receives
        // a simple ConstantExpression of WindowDef or TimeSpan
        object? value = Expression.Lambda(argument).Compile().DynamicInvoke();
        Expression constExpr = value switch
        {
            WindowDef def => Expression.Constant(def, typeof(WindowDef)),
            TimeSpan ts => Expression.Constant(ts, typeof(TimeSpan)),
            _ => argument
        };

        var builder = new WindowClauseBuilder();

        try
        {
            var content = builder.Build(constExpr);
            return $"WINDOW {content}";
        }
        catch (InvalidOperationException ex)
        {
            throw new NotSupportedException(ex.Message, ex);
        }
    }
}
