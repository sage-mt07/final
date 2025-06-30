using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Pipeline;
/// <summary>
/// クエリ実行モード
/// 設計理由：Pull Query（一回限り）とPush Query（ストリーミング）の区別
/// </summary>
internal enum QueryExecutionMode
{
    /// <summary>
    /// Pull Query - 一回限りのクエリ実行
    /// </summary>
    PullQuery,

    /// <summary>
    /// Push Query - 継続的なストリーミングクエリ
    /// </summary>
    PushQuery
}
/// <summary>
/// クエリ組み立て文脈情報
/// 設計理由：Generator層での文脈管理統一、Builder結果の統合情報を保持
/// </summary>
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

/// <summary>
/// クエリパーツ情報
/// 設計理由：Generator層でのKSQL文構成部品管理
/// </summary>
internal record QueryPart(
    string Content,
    bool IsRequired,
    int Order = 0)
{
    /// <summary>
    /// 空のクエリパーツ
    /// </summary>
    public static QueryPart Empty => new(string.Empty, false);

    /// <summary>
    /// 必須クエリパーツ作成
    /// </summary>
    public static QueryPart Required(string content, int order = 0)
    {
        return new QueryPart(content, true, order);
    }

    /// <summary>
    /// 任意クエリパーツ作成
    /// </summary>
    public static QueryPart Optional(string content, int order = 0)
    {
        return new QueryPart(content, !string.IsNullOrWhiteSpace(content), order);
    }

    /// <summary>
    /// 有効性チェック
    /// </summary>
    public bool IsValid => IsRequired && !string.IsNullOrWhiteSpace(Content);

    /// <summary>
    /// 条件付き有効性チェック
    /// </summary>
    public bool IsValidOrOptional => IsValid || (!IsRequired && !string.IsNullOrWhiteSpace(Content));
}

/// <summary>
/// クエリ組み立て結果
/// 設計理由：Generator層での組み立て結果統一管理
/// </summary>
internal record QueryAssemblyResult(
    string FinalQuery,
    QueryAssemblyContext Context,
    List<QueryPart> Parts,
    DateTime AssembledAt,
    bool IsValid)
{
    /// <summary>
    /// 成功結果作成
    /// </summary>
    public static QueryAssemblyResult Success(string query, QueryAssemblyContext context, List<QueryPart> parts)
    {
        return new QueryAssemblyResult(query, context, parts, DateTime.UtcNow, true);
    }

    /// <summary>
    /// 失敗結果作成
    /// </summary>
    public static QueryAssemblyResult Failure(string error, QueryAssemblyContext context)
    {
        return new QueryAssemblyResult(error, context, new List<QueryPart>(), DateTime.UtcNow, false);
    }

    /// <summary>
    /// 組み立て統計情報
    /// </summary>
    public QueryAssemblyStats GetStats()
    {
        return new QueryAssemblyStats(
            TotalParts: Parts.Count,
            RequiredParts: Parts.Count(p => p.IsRequired),
            OptionalParts: Parts.Count(p => !p.IsRequired),
            QueryLength: FinalQuery.Length,
            AssemblyTime: AssembledAt
        );
    }
}

/// <summary>
/// クエリ組み立て統計情報
/// 設計理由：パフォーマンス監視とデバッグ支援
/// </summary>
internal record QueryAssemblyStats(
    int TotalParts,
    int RequiredParts,
    int OptionalParts,
    int QueryLength,
    DateTime AssemblyTime)
{
    /// <summary>
    /// 統計サマリー
    /// </summary>
    public string GetSummary()
    {
        return $"Parts: {TotalParts} (Req:{RequiredParts}, Opt:{OptionalParts}), " +
               $"Length: {QueryLength}, " +
               $"Time: {AssemblyTime:HH:mm:ss.fff}";
    }
}
