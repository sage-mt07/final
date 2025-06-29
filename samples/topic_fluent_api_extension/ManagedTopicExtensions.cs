using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Messaging.Exceptions;
using System;
using System.Collections.Concurrent;

namespace Samples.TopicFluentApiExtension;

/// <summary>
/// Additional extension demonstrating a hypothetical managed topic flag.
/// </summary>
public static class ManagedTopicExtensions
{
    private static readonly ConcurrentDictionary<EntityModel, bool> _managedFlags = new();
    private static readonly ConcurrentDictionary<string, TopicAttribute> _existingTopics = new();

    public static void RegisterExistingTopic(string topicName, TopicAttribute attribute)
        => _existingTopics[topicName] = attribute;

    public static void ClearRegisteredTopics() => _existingTopics.Clear();

    /// <summary>
    /// Marks the underlying topic as managed by the framework.
    /// This is a sample implementation to keep the demo self contained.
    /// </summary>
    public static IEntityBuilder<T> IsManaged<T>(this IEntityBuilder<T> builder, bool isManaged) where T : class
    {
        if (builder is not EntityModelBuilder<T> concrete)
            throw new ArgumentException("Invalid builder type", nameof(builder));

        var model = concrete.GetModel();

        if (isManaged)
        {
            var topic = model.TopicAttribute?.TopicName ?? model.EntityType.Name.ToLowerInvariant();
            if (_existingTopics.TryGetValue(topic, out var existing))
            {
                var desired = model.TopicAttribute!;
                if (existing.ReplicationFactor != desired.ReplicationFactor ||
                    existing.MinInSyncReplicas != desired.MinInSyncReplicas)
                {
                    throw new KafkaTopicConflictException($"Topic configuration conflict for '{topic}'");
                }
            }
        }

        _managedFlags[model] = isManaged;
        return concrete;
    }

    /// <summary>
    /// Retrieves the managed flag for the given builder's model.
    /// </summary>
    public static bool GetIsManaged<T>(this IEntityBuilder<T> builder) where T : class
    {
        if (builder is not EntityModelBuilder<T> concrete)
            throw new ArgumentException("Invalid builder type", nameof(builder));

        var model = concrete.GetModel();
        return _managedFlags.TryGetValue(model, out var value) && value;
    }
}
