using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Query.Linq;

public interface IJoinableEntitySet<T> where T : class
{
    IJoinResult<T, TInner> Join<TInner, TKey>(
        IEntitySet<TInner> inner,
        Expression<Func<T, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector) where TInner : class;
}
