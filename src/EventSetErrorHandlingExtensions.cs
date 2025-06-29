
using Kafka.Ksql.Linq.Core.Abstractions;
using System;

namespace Kafka.Ksql.Linq;
public static class EventSetErrorHandlingExtensions
{
    /// <summary>
    /// DSLチェーン開始：エラーハンドリング設定
    /// </summary>
    public static IErrorHandlingChain<T> StartErrorHandling<T>(this EventSet<T> eventSet) where T : class
    {
        return new ErrorHandlingChain<T>(eventSet);
    }

    /// <summary>
    /// 直接的なOnError設定（既存APIとの互換性）
    /// </summary>
    public static EventSet<T> OnError<T>(this EventSet<T> eventSet, ErrorAction errorAction) where T : class
    {
        var policy = new ErrorHandlingPolicy
        {
            Action = errorAction
        };

        return eventSet.WithErrorPolicy(policy);
    }
}

/// <summary>
/// DSLチェーン実装クラス
/// 型安全性を保証し、正しい順序での呼び出しを強制
/// </summary>
internal class ErrorHandlingChain<T> : IErrorHandlingChain<T> where T : class
{
    private readonly EventSet<T> _eventSet;

    public ErrorHandlingChain(EventSet<T> eventSet)
    {
        _eventSet = eventSet ?? throw new ArgumentNullException(nameof(eventSet));
    }

    public IMapReadyChain<T> OnError(ErrorAction errorAction)
    {
        return new MapReadyChain<T>(_eventSet, errorAction);
    }
}

/// <summary>
/// Map変換準備チェーン実装
/// </summary>
internal class MapReadyChain<T> : IMapReadyChain<T> where T : class
{
    private readonly EventSet<T> _eventSet;
    private readonly ErrorAction _errorAction;

    public MapReadyChain(EventSet<T> eventSet, ErrorAction errorAction)
    {
        _eventSet = eventSet ?? throw new ArgumentNullException(nameof(eventSet));
        _errorAction = errorAction;
    }

    /// <summary>
    /// ✅ 修正版：Map変換実行（エラーハンドリング適用）
    /// </summary>
    public IRetryReadyChain<TResult> Map<TResult>(Func<T, TResult> mapper) where TResult : class
    {
        if (mapper == null)
            throw new ArgumentNullException(nameof(mapper));

        // ✅ 修正：不正なアスタリスク記号を削除
        // エラーハンドリングポリシーを設定
        var eventSetWithErrorPolicy = _eventSet.OnError(_errorAction);

        // Map変換を適用
        var mappedEventSet = ApplyMapTransformation(eventSetWithErrorPolicy, mapper);

        if (mappedEventSet == null)
            throw new InvalidOperationException("Map transformation failed to create mapped EventSet");

        // ✅ 修正：重複return文を削除
        return new RetryReadyChain<TResult>(mappedEventSet);
    }

    /// <summary>
    /// Map変換の実際の適用処理
    /// </summary>
    private EventSet<TResult> ApplyMapTransformation<TResult>(EventSet<T> eventSet, Func<T, TResult> mapper)
        where TResult : class
    {
        // TODO: 実際のMap変換ロジックを実装
        // 現在は仮実装として新しいMappedEventSetを作成

        try
        {
            // 元のEventSetからデータを取得してMap変換
            var mappedItems = new System.Collections.Generic.List<TResult>();

            // 同期版Map変換（仮実装）
            // 実際の実装では非同期処理やストリーミング処理が必要

            // MappedEventSetを作成して返却
            return new MappedEventSet<TResult>(
                mappedItems,
                eventSet.GetContext(),
                eventSet.GetEntityModel());
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Map transformation failed from {typeof(T).Name} to {typeof(TResult).Name}", ex);
        }
    }
}

/// <summary>
/// リトライ準備チェーン実装
/// </summary>
internal class RetryReadyChain<T> : IRetryReadyChain<T> where T : class
{
    private readonly EventSet<T> _eventSet;

    public RetryReadyChain(EventSet<T> eventSet)
    {
        _eventSet = eventSet ?? throw new ArgumentNullException(nameof(eventSet));
    }

    /// <summary>
    /// リトライ設定（Mapに適用）
    /// </summary>
    public EventSet<T> WithRetry(int retryCount = 3, TimeSpan? retryInterval = null)
    {
        if (retryCount < 0)
            throw new ArgumentException("Retry count cannot be negative", nameof(retryCount));

        // リトライポリシーを適用
        var retryPolicy = new ErrorHandlingPolicy
        {
            Action = ErrorAction.Retry,
            RetryCount = retryCount,
            RetryInterval = retryInterval ?? TimeSpan.FromSeconds(1)
        };

        return _eventSet.WithErrorPolicy(retryPolicy);
    }

    /// <summary>
    /// リトライなしで完了
    /// </summary>
    public EventSet<T> Build()
    {
        return _eventSet;
    }
}
