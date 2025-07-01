using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Query.Linq;

public interface IJoinResult<TOuter, TInner, TThird>
    where TOuter : class
    where TInner : class
    where TThird : class
{
    IEntitySet<TResult> Select<TResult>(Expression<Func<TOuter, TInner, TThird, TResult>> resultSelector) where TResult : class;
}
