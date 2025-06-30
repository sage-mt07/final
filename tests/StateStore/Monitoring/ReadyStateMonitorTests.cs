using Confluent.Kafka;
using Kafka.Ksql.Linq.StateStore.Monitoring;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.StateStore.Monitoring;

public class ReadyStateMonitorTests
{
    private class DummyConsumer : IConsumer<object, object>
    {
        public List<TopicPartition> Assignment { get; set; } = new();
        public void Assign(TopicPartition partition) { }
        public void Assign(IEnumerable<TopicPartitionOffset> partitions) { }
        public void Assign(IEnumerable<TopicPartition> partitions) { }
        public void Close() { }
        public void Dispose() { }
        public int AddBrokers(string brokers) => 0;
        public void Pause(IEnumerable<TopicPartition> partitions) { }
        public void Poll(TimeSpan timeout) { }
        public ConsumeResult<object, object>? Consume(CancellationToken cancellationToken = default) => null;
        public ConsumeResult<object, object>? Consume(TimeSpan timeout) => null;
        public void Resume(IEnumerable<TopicPartition> partitions) { }
        public void Seek(TopicPartitionOffset tpo) { }
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => new WatermarkOffsets(0, 0);
        public Offset Position(TopicPartition partition) => new Offset(0);
        public List<TopicPartition> Subscription => new();
        public void StoreOffset(ConsumeResult<object, object> result) { }
        public void Subscribe(string topic) { }
        public void Subscribe(IEnumerable<string> topics) { }
        public void Unassign() { }
        public void Unsubscribe() { }
        public string MemberId => "";
        public Handle Handle => throw new NotImplementedException();
        public string Name => "dummy";
        public string ConsumerGroupMetadata => "";
        public event EventHandler<Error>? Error;
        public void OnError(Error error) => Error?.Invoke(this, error);
        public List<TopicPartition> Assignment { get; } = new();
        public List<TopicPartition> Commit(IEnumerable<TopicPartitionOffset> offsets) => new();
        public List<TopicPartitionOffset> Committed(TimeSpan timeout) => new();
        public void Commit(ConsumeResult<object, object> result) { }
        public void Commit(IEnumerable<TopicPartitionOffset> offsets) { }
    }

    private class TestMonitor : ReadyStateMonitor
    {
        public TestMonitor() : base(new DummyConsumer(), "t", NullLoggerFactory.Instance) { }
        public void TriggerReady() => typeof(ReadyStateMonitor).GetMethod("OnReadyStateChanged", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .Invoke(this, new object[] { new ReadyStateChangedEventArgs { TopicName = "t", IsReady = true } });
    }

    [Fact]
    public async Task WaitUntilReadyAsync_CompletesWhenTriggered()
    {
        var monitor = new TestMonitor();
        var task = monitor.WaitUntilReadyAsync(TimeSpan.FromSeconds(1));
        monitor.TriggerReady();
        var result = await task;
        Assert.True(result);
    }
}
