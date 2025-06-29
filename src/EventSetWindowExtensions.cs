using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kafka.Ksql.Linq;

/// <summary>
/// Window DSL extension methods for EventSet
/// </summary>
public static class EventSetWindowExtensions
{
    private static readonly MethodInfo WindowMethod = typeof(EventSetWindowExtensions)
        .GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
        .First(m => m.Name == nameof(Window) && m.GetParameters().Length == 2 && m.GetParameters()[1].ParameterType == typeof(WindowDef));

    private static readonly MethodInfo WindowDurationMethod = typeof(EventSetWindowExtensions)
        .GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
        .First(m => m.Name == nameof(Window) && m.GetParameters().Length == 2 && m.GetParameters()[1].ParameterType == typeof(TimeSpan));

    public static IQueryable<T> Window<T>(this IQueryable<T> source, WindowDef windowDef) where T : class
    {
        if (source is null) throw new ArgumentNullException(nameof(source));
        if (windowDef is null) throw new ArgumentNullException(nameof(windowDef));

        var call = Expression.Call(
            null,
            WindowMethod.MakeGenericMethod(typeof(T)),
            source.Expression,
            Expression.Constant(windowDef, typeof(WindowDef)));

        return source.Provider.CreateQuery<T>(call);
    }

    public static IQueryable<T> Window<T>(this IQueryable<T> source, TimeSpan duration) where T : class
    {
        if (source is null) throw new ArgumentNullException(nameof(source));

        var call = Expression.Call(
            null,
            WindowDurationMethod.MakeGenericMethod(typeof(T)),
            source.Expression,
            Expression.Constant(duration, typeof(TimeSpan)));

        return source.Provider.CreateQuery<T>(call);
    }

    public static IQueryable<T> Window<T>(this IQueryable<T> source, int minutes) where T : class
    {
        return Window(source, TimeSpan.FromMinutes(minutes));
    }
}

/// <summary>
/// Represents a chainable window definition used only for LINQ expression generation.
/// </summary>
public class WindowDef
{
    internal readonly List<(string Name, object? Value)> Operations = new();

    public WindowDef TumblingWindow()
    {
        Operations.Add((nameof(TumblingWindow), null));
        return this;
    }

    public WindowDef HoppingWindow()
    {
        Operations.Add((nameof(HoppingWindow), null));
        return this;
    }

    public WindowDef SessionWindow()
    {
        Operations.Add((nameof(SessionWindow), null));
        return this;
    }

    public WindowDef Size(TimeSpan ts)
    {
        Operations.Add((nameof(Size), ts));
        return this;
    }

    public WindowDef AdvanceBy(TimeSpan ts)
    {
        Operations.Add((nameof(AdvanceBy), ts));
        return this;
    }

    public WindowDef Gap(TimeSpan ts)
    {
        Operations.Add((nameof(Gap), ts));
        return this;
    }

    public WindowDef Retention(TimeSpan ts)
    {
        Operations.Add((nameof(Retention), ts));
        return this;
    }

    public WindowDef GracePeriod(TimeSpan ts)
    {
        Operations.Add((nameof(GracePeriod), ts));
        return this;
    }

    public WindowDef EmitFinal()
    {
        Operations.Add((nameof(EmitFinal), null));
        return this;
    }
}

public static class TumblingWindow
{
    public static WindowDef Of(TimeSpan size) => new WindowDef().TumblingWindow().Size(size);
    public static WindowDef OfMinutes(int minutes) => Of(TimeSpan.FromMinutes(minutes));
}

public static class HoppingWindow
{
    public static WindowDef Of(TimeSpan size) => new WindowDef().HoppingWindow().Size(size);
    public static WindowDef OfMinutes(int minutes) => Of(TimeSpan.FromMinutes(minutes));
}

public static class SessionWindow
{
    public static WindowDef Of(TimeSpan gap) => new WindowDef().SessionWindow().Gap(gap);
    public static WindowDef OfMinutes(int minutes) => Of(TimeSpan.FromMinutes(minutes));
}
