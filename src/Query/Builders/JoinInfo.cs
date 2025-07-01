using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;
/// <summary>
/// JOIN情報クラス
/// </summary>
internal class JoinInfo
{
    public string OuterType { get; set; } = string.Empty;
    public string InnerType { get; set; } = string.Empty;
    public List<string> OuterKeys { get; set; } = new();
    public List<string> InnerKeys { get; set; } = new();
    public List<string> Projections { get; set; } = new();
    public string OuterAlias { get; set; } = "o";
    public string InnerAlias { get; set; } = "i";
}
