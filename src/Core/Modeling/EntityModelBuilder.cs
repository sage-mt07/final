using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;

namespace Kafka.Ksql.Linq.Core.Modeling;

public class EntityModelBuilder<T> : IEntityBuilder<T> where T : class
{
    private readonly EntityModel _entityModel;

    internal EntityModelBuilder(EntityModel entityModel)
    {
        _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
    }
    public IEntityBuilder<T> AsTable(string? topicName = null, bool useCache = true)
    {
        _entityModel.SetStreamTableType(StreamTableType.Table);
        _entityModel.EnableCache = useCache;
        if (!string.IsNullOrWhiteSpace(topicName))
        {
            _entityModel.TopicAttribute = new TopicAttribute(topicName);
        }
        return this;
    }
    public IEntityBuilder<T> AsStream()
    {
        _entityModel.SetStreamTableType(StreamTableType.Stream);
        return this;
    }
    public IEntityBuilder<T> WithManualCommit()
    {
        _entityModel.UseManualCommit = true;
        return this;
    }

    public EntityModelBuilder<T> OnError(ErrorAction action)
    {
        _entityModel.ErrorAction = action;
        return this;
    }


    public EntityModelBuilder<T> WithPartitions(int partitions)
    {
        if (partitions <= 0)
            throw new ArgumentException("Partitions must be greater than 0", nameof(partitions));

        var attr = EnsureTopicAttribute();
        attr.PartitionCount = partitions;
        return this;
    }

    public EntityModelBuilder<T> WithReplicationFactor(int replicationFactor)
    {
        if (replicationFactor <= 0)
            throw new ArgumentException("ReplicationFactor must be greater than 0", nameof(replicationFactor));

        var attr = EnsureTopicAttribute();
        attr.ReplicationFactor = replicationFactor;
        return this;
    }


    public EntityModelBuilder<T> WithPartitioner(string partitioner)
    {
        if (string.IsNullOrWhiteSpace(partitioner))
            throw new ArgumentException("Partitioner cannot be null or empty", nameof(partitioner));

        var attr = EnsureTopicAttribute();
        attr.Partitioner = partitioner;
        return this;
    }

    public EntityModel GetModel()
    {
        return _entityModel;
    }

    [Obsolete("POCO属性主導型では、Fluent APIでのトピック名変更は禁止されています。[Topic]属性を使用してください。", true)]
    public EntityModelBuilder<T> HasTopicName(string topicName)
    {
        throw new NotSupportedException("POCO属性主導型では、Fluent APIでのトピック名変更は禁止されています。[Topic]属性を使用してください。");
    }

    [Obsolete("POCO属性主導型では、Fluent APIでのキー変更は禁止されています。[Key]属性を使用してください。", true)]
    public EntityModelBuilder<T> HasKey<TKey>(System.Linq.Expressions.Expression<Func<T, TKey>> keyExpression)
    {
        throw new NotSupportedException("POCO属性主導型では、Fluent APIでのキー変更は禁止されています。[Key]属性を使用してください。");
    }

    public override string ToString()
    {
        var entityName = _entityModel.EntityType.Name;
        var topicName = _entityModel.TopicAttribute?.TopicName ?? "未定義";
        var keyCount = _entityModel.KeyProperties.Length;
        var validStatus = _entityModel.IsValid ? "有効" : "無効";

        return $"Entity: {entityName}, Topic: {topicName}, Keys: {keyCount}, Status: {validStatus}";
    }

    private TopicAttribute EnsureTopicAttribute()
    {
        if (_entityModel.TopicAttribute == null)
        {
            _entityModel.TopicAttribute = new TopicAttribute(_entityModel.EntityType.Name.ToLowerInvariant());
        }

        return _entityModel.TopicAttribute;
    }
}
