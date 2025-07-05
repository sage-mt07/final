using System;
using System.Threading;

namespace Kafka.Ksql.Linq.Core.Context;

/// <summary>
/// Provides a scope that marks execution as part of OnModelCreating.
/// Query builder APIs like Where/GroupBy/Select are allowed only within this scope.
/// </summary>
internal static class ModelCreationScope
{
    private static readonly AsyncLocal<bool> _flag = new();

    public static bool IsActive => _flag.Value;

    public static IDisposable Enter()
    {
        _flag.Value = true;
        return new Scope();
    }

    public static void EnsureActive(string apiName)
    {
        if (!IsActive)
        {
            throw new InvalidOperationException($"{apiName} のクエリ定義はOnModelCreating専用です。");
        }
    }

    private sealed class Scope : IDisposable
    {
        public void Dispose()
        {
            _flag.Value = false;
        }
    }
}
