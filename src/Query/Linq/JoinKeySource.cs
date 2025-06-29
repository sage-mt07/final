namespace Kafka.Ksql.Linq.Query.Linq;

internal enum JoinKeySource
{
    Outer,  // 最初のテーブルからのキー
    Inner   // 2番目のテーブルからのキー
}
