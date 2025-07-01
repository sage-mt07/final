using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Pipeline;
internal record QueryAssemblyContext(
    string BaseObjectName,
    QueryExecutionMode ExecutionMode,
    bool IsPullQuery,
    Dictionary<string, object> Metadata)
{
    /// <summary>
    /// デフォルトコンストラクタ
    /// </summary>
    public QueryAssemblyContext(string baseObjectName, QueryExecutionMode executionMode)
        : this(baseObjectName, executionMode, executionMode == QueryExecutionMode.PullQuery, new Dictionary<string, object>())
    {
    }

    /// <summary>
    /// シンプルコンストラクタ
    /// </summary>
    public QueryAssemblyContext(string baseObjectName, bool isPullQuery = true)
        : this(baseObjectName, isPullQuery ? QueryExecutionMode.PullQuery : QueryExecutionMode.PushQuery, isPullQuery, new Dictionary<string, object>())
    {
    }

    /// <summary>
    /// メタデータ追加
    /// </summary>
    public QueryAssemblyContext WithMetadata(string key, object value)
    {
        var newMetadata = new Dictionary<string, object>(Metadata) { [key] = value };
        return this with { Metadata = newMetadata };
    }

    /// <summary>
    /// 実行モード変更
    /// </summary>
    public QueryAssemblyContext WithExecutionMode(QueryExecutionMode mode)
    {
        return this with
        {
            ExecutionMode = mode,
            IsPullQuery = mode == QueryExecutionMode.PullQuery
        };
    }

    /// <summary>
    /// ベースオブジェクト変更
    /// </summary>
    public QueryAssemblyContext WithBaseObject(string baseObjectName)
    {
        return this with { BaseObjectName = baseObjectName };
    }

    /// <summary>
    /// メタデータ取得（型安全）
    /// </summary>
    public T? GetMetadata<T>(string key)
    {
        if (Metadata.TryGetValue(key, out var value) && value is T typedValue)
        {
            return typedValue;
        }
        return default;
    }

    /// <summary>
    /// メタデータ存在チェック
    /// </summary>
    public bool HasMetadata(string key)
    {
        return Metadata.ContainsKey(key);
    }

    /// <summary>
    /// 文脈情報の複製
    /// </summary>
    public QueryAssemblyContext Copy()
    {
        return this with { Metadata = new Dictionary<string, object>(Metadata) };
    }

    /// <summary>
    /// デバッグ情報文字列
    /// </summary>
    public string GetDebugInfo()
    {
        var metadataInfo = Metadata.Count > 0
            ? string.Join(", ", Metadata.Select(kvp => $"{kvp.Key}={kvp.Value}"))
            : "none";

        return $"BaseObject: {BaseObjectName}, " +
               $"Mode: {ExecutionMode}, " +
               $"IsPull: {IsPullQuery}, " +
               $"Metadata: [{metadataInfo}]";
    }
}
