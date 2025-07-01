using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// クエリ句タイプ列挙
/// </summary>
internal enum QueryClauseType
{
    Select,
    From,
    Join,
    Where,
    GroupBy,
    Having,
    Window,
    OrderBy,
    Limit,
    EmitChanges
}

