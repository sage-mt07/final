### AvroCoreInternalTests

- `public void AvroField_PropertyAccessors()`
- `public void AvroSchemaInfo_RegisteredAt_Getter()`
- `public void AvroSchema_PropertyAccessors()`
- `public void AvroValueSerializer_And_Deserializer_RoundTrip()`
- `public void GuidKeySerializer_And_Deserializer()`
- `public void IntKeySerializer_And_Deserializer()`
- `public void LongKeySerializer_And_Deserializer()`

### AvroDeserializer<T>

- `public T Deserialize(ReadOnlySpan<byte> data)`
- `public void Dispose()`

### AvroEntityConfiguration

- `public AvroEntityConfiguration Clone()`
- `public PropertyInfo[] GetIgnoredProperties()`
- `public PropertyInfo[] GetOrderedKeyProperties()`
- `public PropertyInfo[] GetSerializableProperties()`
- `public Type DetermineKeyType()`
- `public ValidationResult Validate()`
- `public bool HasKeys()`
- `public bool IsCompositeKey()`
- `public override bool Equals(object? obj)`
- `public override int GetHashCode()`
- `public override string ToString()`
- `public string GetEffectiveTopicName()`
- `public string GetStreamTableType()`
- `public string GetSummary()`

### AvroEntityConfigurationBuilder<T>

- `public AvroEntityConfiguration Build()`
- `public AvroEntityConfigurationBuilder<T> AsStream()`
 - `public AvroEntityConfigurationBuilder<T> AsTable(string? topicName = null, bool useCache = true)`
- `public AvroEntityConfigurationBuilder<T> EnableCaching(bool enable = true)`
- `public AvroEntityConfigurationBuilder<T> HasKey<TKey>(System.Linq.Expressions.Expression<Func<T, TKey>> keyExpression)`
- `public AvroEntityConfigurationBuilder<T> ToTopic(string topicName)`
- `public AvroEntityConfigurationBuilder<T> ValidateOnStartup(bool validate = true)`
- `public AvroEntityConfigurationBuilder<T> WithPartitions(int partitions)`
- `public AvroEntityConfigurationBuilder<T> WithReplicationFactor(int replicationFactor)`

### AvroEntityConfigurationBuilderTests

- `public void AsStream_SetsCustomSetting()`
- `public void AsTable_SetsCustomSetting()`
- `public void Build_ReturnsConfiguration()`
- `public void Constructor_NullConfiguration_Throws()`
- `public void EnableCaching_SetsFlag()`
- `public void ExtractProperties_SupportsComposite()`
- `public void HasKey_SetsKeyProperties()`
- `public void ToTopic_SetsName()`
- `public void ValidateOnStartup_SetsFlag()`
- `public void WithPartitions_SetsValue()`
- `public void WithReplicationFactor_SetsValue()`

### AvroEntityConfigurationExtensions

- `public static AvroEntityConfigurationBuilder<T> Configure<T>(this AvroEntityConfiguration configuration) where T : class`
- `public static bool IsStreamType(this AvroEntityConfiguration configuration)`
- `public static bool IsTableType(this AvroEntityConfiguration configuration)`

### AvroEntityConfigurationExtensionsTests

- `public void Configure_ReturnsBuilder()`
- `public void Configure_WrongType_Throws()`
- `public void IsStreamType_And_IsTableType()`

### AvroEntityConfigurationTests

- `public void Clone_CreatesEqualCopy()`
- `public void Constructor_AppliesAttributes()`
- `public void DetermineKeyType_ReturnsPropertyType()`
- `public void GetIgnoredProperties_ReturnsIgnored()`
- `public void GetOrderedKeyProperties_SortsByOrder()`
- `public void GetSerializableProperties_ExcludesIgnored()`
- `public void GetSummary_IncludesInfo()`
- `public void IsCompositeKey_ReturnsTrue()`
- `public void ToString_ReturnsSummary()`
- `public void Validate_WarnsOnBadKeyType()`

### AvroExceptionTests

- `public void AvroSchemaRegistrationException_Ctor_SetsMessage()`
- `public void AvroSchemaRegistrationException_Ctor_WithInner()`
- `public void DetermineOperationalAction_ReturnsExpected()`
- `public void FormatFatalMessage_IncludesDetails()`
- `public void SchemaRegistrationFatalException_PropertiesAndSummary()`

### AvroRetryPolicy

- `public void Validate()`

### AvroRetryPolicyTests

- `public void Validate_InvalidDelays_Throws()`
- `public void Validate_InvalidMaxAttempts_Throws()`
- `public void Validate_InvalidValues_Throws(int attempts, int initMs, int maxMs, double backoff)`

### AvroSchemaBuilderDetailedTests

- `public async Task GetKeySchemaAsync_ReturnsSchema()`
- `public async Task GetSchemasAsync_ReturnsKeyAndValue()`
- `public async Task GetValueSchemaAsync_ReturnsValueSchema()`
- `public void GenerateCompositeKeySchema_BuildsRecord()`
- `public void GenerateFieldsFromProperties_ExcludesIgnored()`
- `public void GeneratePrimitiveSchema_ReturnsExpected(Type type, string expected)`
- `public void GenerateValueSchema_CreatesRecord()`
- `public void GetBasicAvroType_ReturnsLogicalTypeForSpecials()`
- `public void GetSchemaProperties_SkipsIgnored()`
- `public void GetTopicName_ReturnsAttributeValue()`
- `public void IsNullableReferenceType_DetectsCorrectly()`
- `public void MapPropertyToAvroType_ReturnsNullableArrayForReference()`
- `public void SerializeSchema_UsesCamelCase()`
- `public void ValidateAvroSchema_ReturnsFalseForEmpty()`
- `public void ValidateAvroSchema_ReturnsFalseForInvalidJson()`
- `public void ValidateAvroSchema_ReturnsFalseForInvalidRecord()`
- `public void ValidateAvroSchema_ReturnsTrueForArray()`
- `public void ValidateAvroSchema_ReturnsTrueForPrimitiveString()`
- `public void ValidateAvroSchema_ReturnsTrueForValidRecord()`

### AvroSchemaBuilderTests

- `public async Task ValidateSchemaAsync_InvalidSchema_ReturnsFalse()`
- `public void GenerateKeySchema_ForSingleKey_ReturnsPrimitiveSchema()`

### AvroSchemaInfoExtensionsTests

- `public void GetKeyTypeName_ReturnsComposite_WhenMultipleKeyProps()`
- `public void GetStreamTableType_UsesHasCustomKey()`
- `public void Subjects_ReturnExpectedStrings()`

### AvroSchemaInfoTests

- `public void HasCustomKey_And_KeyType_Work()`
- `public void ToString_ContainsInfo()`

### AvroSchemaRegistrationServiceTests

- `public async Task GetAllRegisteredSchemasAsync_ReturnsList()`
- `public async Task RegisterAllSchemasAsync_RegistersAndStores()`
- `public void Constructor_NullClient_Throws()`

### AvroSchemaRepositoryTests

- `public void StoreAndRetrieve_Works()`

### AvroSchemaVersionManager

- `public async Task<SchemaUpgradeResult> UpgradeAsync<T>() where T : class`
- `public async Task<System.Collections.Generic.List<int>> GetVersionHistoryAsync<T>() where T : class`
- `public async Task<bool> CanUpgradeAsync<T>(string newSchema) where T : class`
- `public async Task<int> GetLatestVersionAsync<T>() where T : class`

### AvroSchemaVersionManagerTests

- `public async Task CanUpgradeAsync_ReturnsClientValue()`
- `public async Task GetLatestVersionAsync_ReturnsVersion()`
- `public async Task GetVersionHistoryAsync_ReturnsValues()`
- `public async Task UpgradeAsync_Incompatible_ReturnsFailure()`
- `public async Task UpgradeAsync_Success_ReturnsNewId()`
- `public void GetTopicName_UsesAttribute()`

### AvroSerializationManager<T>

- `public SerializationStatistics GetStatistics()`
- `public async Task<(string keySchema, string valueSchema)> GetCurrentSchemasAsync()`
- `public async Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default)`
- `public async Task<SchemaUpgradeResult> UpgradeSchemaAsync(CancellationToken cancellationToken = default)`
- `public async Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default)`
- `public async Task<bool> CanUpgradeSchemaAsync(CancellationToken cancellationToken = default)`
- `public async Task<bool> ValidateRoundTripAsync(T entity, CancellationToken cancellationToken = default)`
- `public void ClearCache()`
- `public void Dispose()`

### AvroSerializationManagerTests

- `public async Task CanUpgradeSchemaAsync_ReturnsValue()`
- `public async Task ClearCache_RemovesManager()`
- `public async Task Dispose_ClearsCache()`
- `public async Task GetCurrentSchemasAsync_ReturnsSchemas()`
- `public async Task GetSerializers_And_Deserializers_Work()`
- `public async Task GetStatistics_ReflectsOperations()`
- `public async Task UpgradeSchemaAsync_Success_ClearsCache()`
- `public async Task ValidateRoundTripAsync_ReturnsTrue()`

### AvroSerializer<T>

- `public void Dispose()`
- `public void Serialize(T value, Stream stream)`

### AvroSerializerCacheTests

- `public async Task GetDeserializersAsync_CachesResults()`
- `public async Task GetSerializersAsync_CachesResults()`
- `public async Task ManagerDispose_ClearsCaches()`
- `public async Task ValidateRoundTripAsync_ReturnsTrue()`
- `public void AvroCompositeKeyDeserializer_IsNull_ReturnsEmpty()`
- `public void AvroCompositeKeySerializer_RoundTrip()`
- `public void AvroCompositeKeySerializer_WrongType_Throws()`
- `public void AvroDeserializer_Ctor_DoesNotThrow()`
- `public void AvroDeserializer_ThrowsOnDeserialize()`
- `public void ClearCache_RemovesManager()`
- `public void Dispose_ClearsManagers()`
- `public void EntityType_ReturnsCorrectType()`
- `public void GenerateCacheKey_UsesTypeName()`
- `public void GetAvroManager_CachesInstance()`
- `public void GetEntityModel_ReturnsKeyInfo()`

### AvroSerializerFactoryPrimitiveTests

- `public void CreatePrimitiveKeyDeserializer_ReturnsExpected(Type type, Type expected)`
- `public void CreatePrimitiveKeyDeserializer_Unsupported_Throws()`
- `public void CreatePrimitiveKeySerializer_ReturnsExpected(Type type, Type expected)`
- `public void CreatePrimitiveKeySerializer_Unsupported_Throws()`

### AvroTimestampAttribute

- `public override string ToString()`

### BigBangScenarioTests

- `public void Build_ComplexQuery_GeneratesExpectedClauses()`

### BuilderUtil

- `public static MemberExpression? ExtractMemberExpression(Expression expression)`

### BuilderUtilTests

- `public void ExtractMemberExpression_UnaryExpression_ReturnsMember()`

### ConsumerInstanceTests

- `public void Properties_CanBeSet()`

### ConsumerKey

- `public bool Equals(ConsumerKey? other)`
- `public override bool Equals(object? obj) => obj is ConsumerKey other && Equals(other)`
- `public override int GetHashCode() => HashCode.Combine(EntityType, TopicName, GroupId)`
- `public override string ToString() => $"{EntityType.Name}:{TopicName}:{GroupId}"`

### CoreExceptionsTests

- `public TestException(string m) : base(m) { }`
- `public TestException(string m, Exception i) : base(m, i) { }`
- `public void CoreConfigurationException_Constructors()`
- `public void CoreException_BaseConstructor_SetsMessage()`
- `public void CoreValidationException_StoresErrors()`
- `public void EntityModelException_Constructors()`

### CoreExtensionsMoreTests

- `public void GetSerializableProperties_ExcludesIgnored()`
- `public void IsKafkaEntity_DetectsProperClass()`
- `public void IsKafkaIgnored_ReturnsTrueForIgnoredProperty()`
- `public void IsNullableProperty_WorksForNullableAndReference()`

### CoreExtensionsTests

- `public void GetTopicName_ReturnsAttributeValue()`
- `public void PropertyExtensions_DetectKey()`
- `public void TypeExtension_ReturnsTopicName()`

### CoreSettingsProviderTests

- `public void GetSettings_ReturnsClone()`
- `public void UpdateSettings_Null_Throws()`
- `public void UpdateSettings_RaisesEventAndClones()`

### CoreSettingsTests

- `public void Validate_AllRequiredFieldsPresent_DoesNotThrow()`
- `public void Validate_MissingFields_Throws()`

### CoreValidationResultTests

- `public void Properties_CanBeSetViaReflection()`

### DDLQueryGeneratorTests

- `public void GenerateCreateStream_CreatesExpectedStatement()`
- `public void GenerateCreateTableAs_WithWhereAndGroupBy()`

### DMLQueryGeneratorTests

- `public void GenerateAggregateQuery_Basic()`
- `public void GenerateAggregateQuery_EarliestByOffset()`
- `public void GenerateAggregateQuery_LatestByOffset()`
- `public void GenerateCountQuery_ReturnsExpected()`
- `public void GenerateSelectAll_WithPushQuery_AppendsEmitChanges()`
- `public void GenerateSelectWithCondition_Basic()`

### DefaultAndMaxLengthAttributeTests

- `public void DefaultValueAttribute_Null_ShowsNullInToString()`
- `public void DefaultValueAttribute_StoresValue()`
- `public void MaxLengthAttribute_InvalidLength_Throws()`
- `public void MaxLengthAttribute_StoresLength()`

### DefaultValueAttribute

- `public override string ToString()`

### DerivedObjectManagerTests

- `public FakeExecutor() : base(new NullLoggerFactory()) { }`
- `public async Task CleanupDerivedObjects_RemovesAll()`
- `public async Task CreateDerivedStreamAsync_AddsObject()`
- `public async Task CreateDerivedTableAsync_AddsObject()`
- `public override Task ExecuteDDLAsync(string ddlQuery) { ExecutedQueries.Add(ddlQuery); return Task.CompletedTask; }`
- `public override Task StopAllQueriesAsync() => Task.CompletedTask`
- `public override Task<List<T>> ExecutePullQueryAsync<T>(string query) => Task.FromResult(new List<T>())`
- `public override Task<List<T>> ExecutePushQueryAsync<T>(string query) => Task.FromResult(new List<T>())`
- `public override void Dispose() { }`
- `public override void ExecuteDDL(string ddlQuery) => ExecutedQueries.Add(ddlQuery)`
- `public void CleanupDerivedObjects_Synchronous()`
- `public void CreateDerivedStream_AddsObjectAndExecutesDDL()`

### DirectoryImportConfig

- `public CsvImportConfig ToCsvConfig(string filePath)`
- `public JsonImportConfig ToJsonConfig(string filePath)`

### DlqConfigurationExtensions

- `public static KsqlDslOptions ConfigureDlq(`
- `public static KsqlDslOptions DisableDlqAutoCreation(this KsqlDslOptions options)`
- `public static KsqlDslOptions WithDlqRetention(`

### DlqEnvelope

- `public override string ToString()`

### DlqTopicConfiguration

- `public string GetSummary()`
- `public void Validate()`

### EntityBuilderTopicExtensions

- `public static IEntityBuilder<T> HasTopic<T>(this IEntityBuilder<T> builder, string topicName) where T : class`
- `public static IEntityBuilder<T> WithMinInSyncReplicas<T>(this IEntityBuilder<T> builder, int minInSyncReplicas) where T : class`
- `public static IEntityBuilder<T> WithPartitioner<T>(this IEntityBuilder<T> builder, string partitioner) where T : class`
- `public static IEntityBuilder<T> WithPartitions<T>(this IEntityBuilder<T> builder, int partitions) where T : class`
- `public static IEntityBuilder<T> WithReplicationFactor<T>(this IEntityBuilder<T> builder, int replicationFactor) where T : class`

### EntityModel

- `public StreamTableType GetExplicitStreamTableType()`
- `public bool HasKeys()`
- `public bool IsCompositeKey()`
- `public void SetStreamTableType(StreamTableType streamTableType)`

### EntityModelBuilder<T>

- `public EntityModel GetModel()`
- `public EntityModelBuilder<T> HasKey<TKey>(System.Linq.Expressions.Expression<Func<T, TKey>> keyExpression)`
- `public EntityModelBuilder<T> HasTopic(string topicName)`
- `public EntityModelBuilder<T> HasTopicName(string topicName)`
- `public EntityModelBuilder<T> OnDeserializationError(DeserializationErrorPolicy policy)`
- `public EntityModelBuilder<T> OnError(ErrorAction action)`
- `public EntityModelBuilder<T> WithMinInSyncReplicas(int minInSyncReplicas)`
- `public EntityModelBuilder<T> WithPartitioner(string partitioner)`
- `public EntityModelBuilder<T> WithPartitions(int partitions)`
- `public EntityModelBuilder<T> WithReplicationFactor(int replicationFactor)`
 - `public IEntityBuilder<T> AsStream()`
 - `public IEntityBuilder<T> AsTable(string? topicName = null, bool useCache = true)`
- `public IEntityBuilder<T> WithManualCommit()`
- `public override string ToString()`

### EntityModelBuilderTests

- `public void Constructor_StoresModel()`
- `public void ObsoleteMethods_ThrowViaReflection()`

### ErrorHandlingContext

- `public async Task<bool> HandleErrorAsync<T>(T originalItem, Exception exception, KafkaMessageContext messageContext)`

### ErrorHandlingExtensions

- `public static EventSet<T> OnError<T>(this EventSet<T> eventSet,`
- `public static EventSet<T> WithRetryWhen<T>(this EventSet<T> eventSet,`

### ErrorHandlingPolicy

- `public static ErrorHandlingPolicy CircuitBreaker(int failureThreshold = 5, TimeSpan recoveryInterval = default)`
- `public static ErrorHandlingPolicy ExponentialBackoff(int maxRetries = 3, TimeSpan baseInterval = default)`

### EventSet<T>

- `public EntityModel GetEntityModel() => _entityModel`
- `public EventSet<T> WithRetry(int maxRetries, TimeSpan? retryInterval = null)`
- `public EventSet<TResult> Map<TResult>(Func<T, TResult> mapper) where TResult : class`
- `public IKsqlContext GetContext() => _context`
- `public MappedEventSet(List<T> mappedItems, IKsqlContext context, EntityModel originalEntityModel, IErrorSink? errorSink = null)`
- `public abstract IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public async Task<EventSet<TResult>> Map<TResult>(Func<T, Task<TResult>> mapper) where TResult : class`
- `public override async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public override async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)`
- `public override string ToString()`
- `public static MappedEventSet<T> Create(List<T> mappedItems, IKsqlContext context, EntityModel originalEntityModel, IErrorSink? errorSink = null)`
- `public static MappedEventSet<T> CreateWithDlq(List<T> mappedItems, IKsqlContext context, EntityModel originalEntityModel, IErrorSink dlqErrorSink)`
- `public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? _entityModel.EntityType.Name`
- `public virtual async IAsyncEnumerable<object> ForEachAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)`
- `public virtual async Task AddAsync(T entity, CancellationToken cancellationToken = default)`
- `public virtual async Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)`
- `public virtual async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)`

### EventSetErrorHandlingExtensions

- `public ErrorHandlingChain(EventSet<T> eventSet)`
- `public EventSet<T> Build()`
- `public EventSet<T> WithRetry(int retryCount = 3, TimeSpan? retryInterval = null)`
- `public IMapReadyChain<T> OnError(ErrorAction errorAction)`
- `public IRetryReadyChain<TResult> Map<TResult>(Func<T, TResult> mapper) where TResult : class`
- `public MapReadyChain(EventSet<T> eventSet, ErrorAction errorAction)`
- `public RetryReadyChain(EventSet<T> eventSet)`
- `public static EventSet<T> OnError<T>(this EventSet<T> eventSet, ErrorAction errorAction) where T : class`
- `public static IErrorHandlingChain<T> StartErrorHandling<T>(this EventSet<T> eventSet) where T : class`

### EventSetTests

- `public Dictionary<Type, EntityModel> GetEntityModels() => new()`
- `public TestSet(List<TestEntity> items, EntityModel model) : base(new DummyContext(), model)`
- `public ValueTask DisposeAsync() => ValueTask.CompletedTask`
- `public async Task AddAsync_NullEntity_Throws()`
- `public async Task ForEachAsync_InvokesAction()`
- `public async Task ToListAsync_ReturnsItems()`
- `public object GetEventSet(Type entityType) => throw new NotImplementedException()`
- `public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public void Dispose() { }`
- `public void Metadata_ReturnsExpectedValues()`

### EventSetWindowExtensions

- `public static IQueryable<T> Window<T>(this IQueryable<T> source, TimeSpan duration) where T : class`
- `public static IQueryable<T> Window<T>(this IQueryable<T> source, WindowDef windowDef) where T : class`
- `public static IQueryable<T> Window<T>(this IQueryable<T> source, int minutes) where T : class`

### EventSetWithServicesSendTests

- `public Task FlushAsync(TimeSpan timeout) => Task.CompletedTask`
- `public Task<KafkaBatchDeliveryResult> SendBatchAsync(IEnumerable<T> messages, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)`
- `public Task<KafkaDeliveryResult> SendAsync(T message, KafkaMessageContext? context = null, CancellationToken cancellationToken = default)`
- `public TestContext() : base() { }`
- `public async Task SendEntityAsync_UsesProducerManager()`
- `public void Dispose() { }`
- `public void SetProducer(object manager)`

### EventSetWithServicesTests

- `public TestContext() : base() { }`
- `public void Constructors_CreateInstances()`

### EventSetWithStateStoreKeyTests

- `public Dictionary<Type, EntityModel> GetEntityModels() => new()`
- `public EntityModel GetEntityModel() => new EntityModel { EntityType = typeof(T), TopicAttribute = new TopicAttribute("t"), AllProperties = Array.Empty<System.Reflection.PropertyInfo>(), KeyProperties = Array.Empty<System.Reflection.PropertyInfo>() }`
- `public IKsqlContext GetContext() => null!`
- `public Task AddAsync(T entity, CancellationToken cancellationToken = default) => Task.CompletedTask`
- `public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default) => Task.CompletedTask`
- `public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default) => Task.FromResult(new List<T>())`
- `public ValueTask DisposeAsync() => ValueTask.CompletedTask`
- `public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) { await Task.CompletedTask; yield break; }`
- `public object GetEventSet(Type entityType) => throw new NotImplementedException()`
- `public string GetTopicName() => typeof(T).Name`
- `public void Dispose() { }`
- `public void GenerateEntityKey_MultipleKeys_ConcatenatesValues()`
- `public void GenerateEntityKey_NoKeyProperties_UsesHashCode()`
- `public void GenerateEntityKey_NullEntity_ReturnsGuidLike()`
- `public void GenerateEntityKey_SingleKeyProperty_UsesValue()`

### Example1_Basic

- `public static void Configure(ModelBuilder builder)`

### Example2_ManagedMode

- `public static void Configure(ModelBuilder builder)`
- `public static void Run()`

### Example3_AdvancedOptions

- `public static void Configure(ModelBuilder builder)`

### ExpressionAnalysisResultTests

- `public void HasJoin_PropertyRoundTrip()`

### ForEachAsyncErrorHandlingTests

- `public Dictionary<Type, EntityModel> GetEntityModels() => new()`
- `public FaultyEnumerator(List<TestEntity> items) => _items = items`
- `public FaultySet(List<TestEntity> items, EntityModel model) : base(new DummyContext(), model)`
- `public ValueTask DisposeAsync() => ValueTask.CompletedTask`
- `public ValueTask<bool> MoveNextAsync()`
- `public async Task EnumeratorException_IsSkipped()`
- `public object GetEventSet(Type entityType) => throw new NotImplementedException()`
- `public override IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public void Dispose() { }`

### GroupByBuilderTests

- `public void Build_GroupByMultipleKeys_ReturnsClause()`
- `public void Build_NoKeys_ThrowsInvalidOperationException()`

### HavingBuilderTests

- `public void Build_CountWithoutSelector_ReturnsCountAll()`
- `public void Build_NullExpression_ThrowsArgumentNullException()`
- `public void Build_SumWithLambda_ReturnsAggregateCondition()`
- `public void GetSqlOperator_ReturnsExpected(ExpressionType type, string expected)`
- `public void GetSqlOperator_Unsupported_Throws()`
- `public void TransformMethodName_ReturnsExpected(string original, string expected)`

### HoppingWindow

- `public static WindowDef Of(TimeSpan size) => new WindowDef().HoppingWindow().Size(size)`
- `public static WindowDef OfMinutes(int minutes) => Of(TimeSpan.FromMinutes(minutes))`

### JoinBuilderTests

- `public Holder(object value) { Value = value; }`
- `public void Build_InvalidJoin_ReturnsErrorComment()`
- `public void Build_NullExpression_ThrowsArgumentNullException()`
- `public void Build_SimpleJoin_ReturnsJoinQuery()`
- `public void FindJoinCall_ReturnsMethodCallFromInvocation()`
- `public void FindJoinCall_ReturnsMethodCallFromLambda()`
- `public void FindJoinCall_ReturnsMethodCallFromMemberInit()`
- `public void FindJoinCall_ReturnsMethodCallFromNewExpression()`
- `public void ThreeTableJoin_AllKeysMatch_ReturnsJoinedRecords()`
- `public void ThreeTableJoin_KeyMismatch_ReturnsEmpty()`
- `public void TwoTableJoin_AllKeysMatch_ReturnsJoinedRecords()`
- `public void TwoTableJoin_KeyMismatch_ReturnsEmpty()`

### JoinableEntitySet<T>

- `public EntityModel GetEntityModel() => _baseEntitySet.GetEntityModel()`
- `public EntityModel GetEntityModel() => _entityModel`
- `public EntityModel GetEntityModel() => _inner.GetEntityModel()`
- `public EntitySetAdapter(IEntitySet<T> inner)`
- `public IEntitySet<TResult> Select<TResult>(`
- `public IJoinResult<T, TInner> Join<TInner, TKey>(`
- `public IJoinResult<TOuter, TInner, TThird> Join<TThird, TKey>(`
- `public IKsqlContext GetContext() => _baseEntitySet.GetContext()`
- `public IKsqlContext GetContext() => _context`
- `public IKsqlContext GetContext() => _inner.GetContext()`
- `public JoinResult(`
- `public Task AddAsync(TResult entity, CancellationToken cancellationToken = default)`
- `public Task ForEachAsync(Func<TResult, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)`
- `public ThreeWayJoinResult(`
- `public TypedJoinResultEntitySet(`
- `public TypedThreeWayJoinResultEntitySet(`
- `public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public async IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public async IAsyncEnumerator<object> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public async Task AddAsync(T entity, CancellationToken cancellationToken = default)`
- `public async Task AddAsync(object entity, CancellationToken cancellationToken = default)`
- `public async Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)`
- `public async Task ForEachAsync(Func<object, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)`
- `public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)`
- `public async Task<List<TResult>> ToListAsync(CancellationToken cancellationToken = default)`
- `public async Task<List<object>> ToListAsync(CancellationToken cancellationToken = default)`
- `public override string ToString()`
- `public string GetTopicName() => _baseEntitySet.GetTopicName()`
- `public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? typeof(TResult).Name`
- `public string GetTopicName() => _inner.GetTopicName()`

### JsonAssert

- `public static void Equal(string expected, string actual)`

### JsonAssertTests

- `public void Equal_IgnoresFormattingAndCase()`

### KafkaBatch<TValue

- `public Task CommitAsync() => Task.CompletedTask`

### KafkaBatchOptionsTests

- `public void Defaults_AreExpected()`

### KafkaBatchTests

- `public async Task CommitAsync_Completes()`
- `public void IsEmpty_WhenNoMessages_ReturnsTrue()`
- `public void ProcessingTime_ComputedCorrectly()`

### KafkaConsumerManagerExceptionTests

- `public void Constructors_SetProperties()`

### KafkaConsumerManagerTests

- `public void BuildConsumerConfig_ReturnsConfiguredValues()`
- `public void GetEntityModel_ReturnsModelWithAttributes()`
- `public void GetOrCreateSerializationManager_CachesInstance()`

### KafkaContextCore

- `public Dictionary<Type, EntityModel> GetEntityModels()`
- `public IEntitySet<T> Set<T>() where T : class`
- `public object GetEventSet(Type entityType)`
- `public override string ToString()`
- `public virtual async ValueTask DisposeAsync()`
- `public void Dispose()`

### KafkaIgnoreAttribute

- `public override string ToString()`

### KafkaProducerManagerExtraTests

- `public void BuildProducerConfig_WithSecurityAndPartitioner()`
- `public void CreateSchemaRegistryClient_UsesOptions()`
- `public void CreateSchemaRegistryClient_WithAuthAndSsl()`

### KafkaProducerManagerTests

- `public void BuildProducerConfig_ReturnsConfiguredValues()`
- `public void GetEntityModel_ReturnsModelWithAttributes()`
- `public void GetOrCreateSerializationManager_CachesInstance()`

### KeyAttribute

- `public override string ToString()`

### KeyExtractorTests

- `public void DetermineKeyType_WithCompositeKeys_ReturnsDictionaryType()`
- `public void ExtractKeyProperties_ReturnsOrderedProperties()`
- `public void ExtractKeyValue_WithEntity_ReturnsDictionary()`
- `public void IsCompositeKey_WithMultipleKeys_ReturnsTrue()`
- `public void IsSupportedKeyType_ReturnsExpectedResults()`
- `public void KeyToString_WithDictionary_ReturnsConcatenatedString()`
- `public void KeyToString_WithNull_ReturnsEmpty()`
- `public void KeyToString_WithObject_UsesToString()`
- `public void KeyToString_WithString_ReturnsSameString()`
- `public void ToAvroEntityConfiguration_MapsProperties()`

### KsqlContext

- `public EntityModel GetEntityModel() => _entityModel`
- `public EventSetWithServices(KsqlContext context, EntityModel entityModel)`
- `public IKsqlContext GetContext() => _ksqlContext`
- `public async IAsyncEnumerable<object> ForEachAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)`
- `public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public async Task AddAsync(T entity, CancellationToken cancellationToken = default)`
- `public async Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)`
- `public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)`
- `public override string ToString()`
- `public string GetDlqTopicName()`
- `public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? typeof(T).Name`

### KsqlContextAsyncTests

- `public TestContext() : base() { }`
- `public async Task DisposeAsyncCore_DisposesManagers()`

### KsqlContextBuilder

- `public KsqlContextBuilder ConfigureValidation(`
- `public KsqlContextBuilder EnableDebugMode(bool enable = true)`
- `public KsqlContextBuilder EnableLogging(ILoggerFactory loggerFactory)`
- `public KsqlContextBuilder UseSchemaRegistry(ISchemaRegistryClient client)`
- `public KsqlContextBuilder UseSchemaRegistry(SchemaRegistryConfig config)`
- `public KsqlContextBuilder UseSchemaRegistry(string url)`
- `public KsqlContextBuilder UseConfiguration(Microsoft.Extensions.Configuration.IConfiguration configuration)`
- `public KsqlContextBuilder WithTimeouts(TimeSpan schemaRegistrationTimeout)`
- `public KsqlContextOptions Build()`
- `public T BuildContext<T>() where T : KsqlContext`
- `public static KsqlContextBuilder Create()`

### KsqlContextBuilderTests

- `public void BuildContext_CreatesInstance()`
- `public void Builder_Methods_ConfigureOptions()`
- `public void Create_ReturnsBuilder()`
- `public void UseConfiguration_SetsOptions()`

### KsqlContextConversionTests

- `public IReadOnlyDictionary<Type, AvroEntityConfiguration> Convert(Dictionary<Type, EntityModel> models)`
- `public TestContext() : base() { }`
- `public void ConvertToAvroConfigurations_CreatesConfiguration()`

### KsqlContextOptions

- `public void Validate()`

### KsqlContextOptionsExtensions

- `public static KsqlContextOptions ConfigureValidation(`
- `public static KsqlContextOptions EnableDebugMode(`
- `public static KsqlContextOptions EnableLogging(`
- `public static KsqlContextOptions UseSchemaRegistry(`
- `public static KsqlContextOptions WithTimeouts(`

### KsqlContextOptionsExtensionsTests

- `public void ConfigureValidation_UpdatesFlags()`
- `public void EnableDebugMode_SetsFlag()`
- `public void EnableLogging_SetsLoggerFactory()`
- `public void UseSchemaRegistry_WithConfig_ConfiguresClient()`
- `public void UseSchemaRegistry_WithUrl_ConfiguresClient()`
- `public void WithTimeouts_SetsTimeout()`

### KsqlContextOptionsTests

- `public void Validate_WithMissingClient_Throws()`
- `public void Validate_WithNonPositiveTimeout_Throws()`
- `public void Validate_WithValidOptions_DoesNotThrow()`

### KsqlContextTests

- `public IEntitySet<T> CallCreateEntitySet<T>(EntityModel model) where T : class`
- `public KafkaConsumerManager CallGetConsumerManager() => base.GetConsumerManager()`
- `public KafkaProducerManager CallGetProducerManager() => base.GetProducerManager()`
- `public TestContext() : base() { }`
- `public TestContext(KafkaContextOptions opt) : base(opt) { }`
- `public void Constructors_InitializeManagers()`
- `public void CreateEntitySet_ReturnsEventSet()`
- `public void Dispose_DoesNotThrow()`

### KsqlDbExecutor

- `public abstract Task ExecuteDDLAsync(string ddlQuery)`
- `public abstract Task StopAllQueriesAsync()`
- `public abstract Task<List<T>> ExecutePullQueryAsync<T>(string query) where T : class`
- `public abstract Task<List<T>> ExecutePushQueryAsync<T>(string query) where T : class`
- `public abstract void Dispose()`
- `public abstract void ExecuteDDL(string ddlQuery)`

### KsqlDbRestApiClientTests

- `public Handler(HttpResponseMessage response) => _response = response`
- `public async Task ExecuteQueryAsync_ParsesResponse()`
- `public async Task ExecuteStatementAsync_ParsesResponse()`

### KsqlDslOptionsTests

- `public void Defaults_AreExpected()`

### LateEarliestOffsetTests

- `public void ProjectionBuilder_EarliestByOffset_GeneratesFunction()`
- `public void ProjectionBuilder_LatestByOffset_GeneratesFunction()`

### LinqJoinExtensions

- `public static IJoinResult<TOuter, TInner> Join<TOuter, TInner, TKey>(`
- `public static IJoinableEntitySet<T> AsJoinable<T>(this IEntitySet<T> entitySet) where T : class`

### LoggerFactoryExtensions

- `public static ILogger CreateLoggerOrNull(this ILoggerFactory? loggerFactory, Type type)`
- `public static ILogger CreateLoggerOrNull(this ILoggerFactory? loggerFactory, string categoryName)`
- `public static ILogger<T> CreateLoggerOrNull<T>(this ILoggerFactory? loggerFactory)`
- `public static void LogDebugWithLegacySupport(this ILogger logger,`
- `public static void LogDebugWithLegacySupport<T>(this ILogger<T> logger,`
- `public static void LogErrorWithLegacySupport<T>(this ILogger<T> logger,`
- `public static void LogInformationWithLegacySupport<T>(this ILogger<T> logger,`
- `public static void LogWarningWithLegacySupport<T>(this ILogger<T> logger,`
- `public static Microsoft.Extensions.Logging.ILoggerFactory CreateLoggerFactory(this Microsoft.Extensions.Configuration.IConfiguration configuration)`

### LoggerFactoryExtensionsTests

- `public void CreateLoggerOrNull_ReturnsNullLoggerWhenFactoryNull()`
- `public void CreateLoggerOrNull_UsesFactory()`
- `public void LogMethods_WithAndWithoutFactory()`
- `public void CreateLoggerFactory_FromConfiguration_RespectsNamespaceLevels()`

### ManagedTopicExtensions

- `public static IEntityBuilder<T> IsManaged<T>(this IEntityBuilder<T> builder, bool isManaged) where T : class`
- `public static bool GetIsManaged<T>(this IEntityBuilder<T> builder) where T : class`
- `public static void ClearRegisteredTopics() => _existingTopics.Clear()`
- `public static void RegisterExistingTopic(string topicName, TopicAttribute attribute)`

### ManagedTopicExtensionsTests

- `public void ManagedFlag_IsTrue_WhenMarked()`

### ManualCommitTests

- `public Dictionary<Type, EntityModel> GetEntityModels() => new()`
- `public ManualCommitSet(List<TestEntity> items, EntityModel model) : base(new DummyContext(), model)`
- `public ValueTask DisposeAsync() => ValueTask.CompletedTask`
- `public async Task ForEachAsync_AutoCommit_ReturnsEntity()`
- `public async Task ForEachAsync_ManualCommit_WrapperAndAck()`
- `public object GetEventSet(Type entityType) => throw new NotImplementedException()`
- `public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)`
- `public void Dispose() { }`

### MaxLengthAttribute

- `public override string ToString()`

### MessagingConfigurationTests

- `public void CommonSection_PropertyGetters()`
- `public void ConsumerSection_PropertyGetter()`
- `public void SchemaRegistrySection_PropertyGetters()`
- `public void TopicCreationSection_PropertyGetters()`
- `public void TopicSection_PropertyGetters()`

### MessagingExceptionTests

- `public void ConsumerPoolException_Constructors()`
- `public void KafkaBatchSendException_SetsBatchResult()`
- `public void KafkaConsumerException_Constructors()`
- `public void KafkaProducerManagerException_Constructors()`
- `public void ProducerPoolException_Constructors()`

### MessagingPropertyTests

- `public void KafkaBatchDeliveryResult_AllSuccessful_WhenNoErrors()`
- `public void KafkaDeliveryResult_Properties_RoundTrip()`
- `public void PooledConsumer_Defaults_Settable()`
- `public void PooledProducer_Defaults_Settable()`

### ModelBuilderTests

- `public void AddAndGetEntityModel_Works()`
- `public void GetAllEntityModels_ReturnsDictionary()`
- `public void GetModelSummary_ContainsEntity()`
- `public void PrivateMethods_Work()`
- `public void ValidateAllModels_ReturnsTrueForValidModel()`

### ModelValidationResult

- `public string GetSummary()`

### ModelValidationResultTests

- `public void GetSummary_FormatsMessages()`
- `public void Properties_DefaultValues()`

### MultiWindowAccessTests

- `public FakeExecutor() : base(new NullLoggerFactory()) { }`
- `public override System.Threading.Tasks.Task ExecuteDDLAsync(string ddlQuery) => System.Threading.Tasks.Task.CompletedTask`
- `public override System.Threading.Tasks.Task StopAllQueriesAsync() => System.Threading.Tasks.Task.CompletedTask`
- `public override void Dispose() { }`
- `public override void ExecuteDDL(string ddlQuery) { }`
- `public void Window_WithDifferentDurations_GeneratesDistinctQueries()`

### OffsetAggregateExtensions

- `public static TResult EarliestByOffset<TSource, TKey, TResult>(this IGrouping<TKey, TSource> source, Expression<Func<TSource, TResult>> selector)`
- `public static TResult LatestByOffset<TSource, TKey, TResult>(this IGrouping<TKey, TSource> source, Expression<Func<TSource, TResult>> selector)`

### ProducerKeyTests

- `public void Constructor_AssignsProperties()`
- `public void Constructor_NullArguments_Throws()`
- `public void Equals_WithSameValues_ReturnsTrue()`
- `public void ToString_ReturnsFormattedString()`

### ProjectionBuilderTests

- `public void Build_NewExpressionWithAlias_ReturnsSelectClause()`
- `public void Build_ParameterExpression_ReturnsSelectAll()`
- `public void Build_ToLowerMethod_ConvertsToFunction()`
- `public void Build_UnsupportedOperator_ThrowsNotSupportedException()`
- `public void GetSqlOperator_UnsupportedOperator_Throws()`

### QueryDiagnosticsTests

- `public void LogStep_And_MarkComplete_NoException()`
- `public void SetMetadata_StoresValue()`

### QueryExecutionPipelineTests

- `public FakeExecutor() : base(new NullLoggerFactory()) { }`
- `public async Task ExecuteQueryAsync_ReturnsResult()`
- `public async Task StopAllStreamingQueriesAsync_CallsExecutor()`
- `public override Task ExecuteDDLAsync(string ddlQuery) => Task.CompletedTask`
- `public override Task StopAllQueriesAsync() { StopCalled = true; return Task.CompletedTask; }`
- `public override void Dispose() { Disposed = true; }`
- `public override void ExecuteDDL(string ddlQuery) { }`
- `public void Dispose_CleansUpAndDisposesExecutor()`
- `public void GenerateKsqlQuery_GroupBySelect_PushQuery()`
- `public void GenerateKsqlQuery_WhereSelect_PullQuery()`
- `public void GetDiagnostics_ReturnsConstant()`
- `public void IsPullQuery_ReflectsDiagnosticsMetadata()`
- `public void ToKsql_CallsGenerateKsqlQuery()`

### QueryExecutionResultTests

- `public void PropertyRoundTrip()`

### ReadyStateInfoTests

- `public void ToString_NotReadyIncludesBindingTime()`
- `public void ToString_ReadyStateIncludesTimeToReady()`

### ResilientAvroSerializerManagerTests

- `public async Task CheckCompatibilityWithRetryAsync_ReturnsValue()`
- `public async Task GetSchemaWithRetryAsync_ReturnsInfo()`
- `public async Task RegisterSchemaWithRetryAsync_ReturnsId()`
- `public void CalculateDelay_AppliesBackoff()`
- `public void ExtractTopicFromSubject_ReturnsTopic(string subject, string expected)`
- `public void ShouldRetry_RespectsPolicy()`

### SchemaGenerationOptions

- `public SchemaGenerationOptions Clone()`

### SchemaGenerationOptionsTests

- `public void Clone_CopiesAllProperties()`

### SchemaGenerationStats

- `public string GetSummary()`

### SchemaRegistrationFatalException

- `public override string ToString()`
- `public string GetOperationalSummary()`

### SchemaRegistryClientWrapperTests

- `public async Task CheckCompatibilityAsync_ReturnsClientValue()`
- `public async Task GetAllSubjectsAsync_ReturnsValues()`
- `public async Task GetSchemaVersionsAsync_ReturnsValues()`
- `public async Task RegisterKeySchemaAsync_UsesTopicKeySubject()`
- `public async Task RegisterSchemaAsync_PassesThrough()`
- `public async Task RegisterTopicSchemasAsync_CallsBoth()`
- `public async Task RegisterValueSchemaAsync_UsesTopicValueSubject()`
- `public void Constructor_NullClient_Throws()`
- `public void Dispose_CallsClientDispose()`
- `public void StringKeyDeserializer_DecodesUtf8()`
- `public void StringKeyDeserializer_Null_ReturnsEmpty()`
- `public void StringKeySerializer_EncodesUtf8()`

### SchemaRegistryTests

- `public FakeExecutor() : base(new NullLoggerFactory()) { }`
- `public async Task Dispose_CallsCleanup()`
- `public async Task GetObjectName_ReturnsName()`
- `public async Task RegisterSchemaAsync_StreamAttribute_UsesCreateStream()`
- `public async Task RegisterSchemaAsync_UsesCreateTableWhenKeysPresent()`
- `public async Task UnregisterAllSchemasAsync_RemovesAll()`
- `public async Task UnregisterSchemaAsync_RemovesRegistration()`
- `public override Task ExecuteDDLAsync(string ddlQuery) { DdlQueries.Add(ddlQuery); return Task.CompletedTask; }`
- `public override Task StopAllQueriesAsync() => Task.CompletedTask`
- `public override Task<List<T>> ExecutePullQueryAsync<T>(string query) => Task.FromResult(new List<T>())`
- `public override Task<List<T>> ExecutePushQueryAsync<T>(string query) => Task.FromResult(new List<T>())`
- `public override void Dispose() { }`
- `public override void ExecuteDDL(string ddlQuery) => DdlQueries.Add(ddlQuery)`

### SchemaUpgradeResultTests

- `public void Properties_Work()`

### SelectBuilderTests

- `public void BuildCondition_BooleanNegation_IncludesParameterPrefix()`
- `public void Build_NullExpression_ThrowsArgumentNullException()`
- `public void Build_SimpleEquality_ReturnsWhereClause()`

### SerializerAbstractionsTests

- `public byte[] Serialize(object data, SerializationContext context) => Array.Empty<byte>()`
- `public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) => new object()`
- `public void DeserializerPair_PropertyAccessors()`
- `public void SerializationStatistics_PropertyAccessors()`
- `public void SerializerPair_PropertyAccessors()`

### SessionWindow

- `public static WindowDef Of(TimeSpan gap) => new WindowDef().SessionWindow().Gap(gap)`
- `public static WindowDef OfMinutes(int minutes) => Of(TimeSpan.FromMinutes(minutes))`

### SpecialTypeHandlingTests

- `public void CharProperty_ProducesWarning()`
- `public void ShortProperty_MappedAsInteger()`

### StreamTableAnalyzerTests

- `public void Analyze_GroupBySelect_ReturnsTableOutput()`
- `public void Analyze_SimpleWhereSelect_ReturnsStreamOutput()`

### TopicAdvancedExtensions

- `public static IEntityBuilder<T> WithCleanupPolicy<T>(this IEntityBuilder<T> builder, string policy) where T : class`
- `public static IEntityBuilder<T> WithRetention<T>(this IEntityBuilder<T> builder, TimeSpan retention) where T : class`

### TopicAttribute

- `public Dictionary<string, object> ToKafkaTopicConfig()`
- `public override string ToString()`

### TopicAttributeTests

- `public void Constructor_SetsTopicName()`
- `public void Partitioner_Property_RoundTrips()`
- `public void ToKafkaTopicConfig_BuildsDictionary()`
- `public void ToString_IncludesDescription()`

### TopicFluentApiTests

- `public void FluentApi_ConfiguresTopicSettings()`

### TopicIsrSettingTests

- `public void FluentApi_StoresMinInSyncReplicas()`
- `public void IsManaged_ThrowsWhenSettingsConflict()`
- `public void MinInSyncReplicas_DefaultsToNull()`

### TumblingWindow

- `public static WindowDef Of(TimeSpan size) => new WindowDef().TumblingWindow().Size(size)`
- `public static WindowDef OfMinutes(int minutes) => Of(TimeSpan.FromMinutes(minutes))`

### UnifiedSchemaGeneratorTests

- `public void GenerateCompositeKeySchema_BuildsRecord()`
- `public void GenerateFieldsFromConfiguration_UsesConfiguration()`
- `public void GenerateFields_CreatesFields()`
- `public void GenerateKeySchema_FromConfiguration()`
- `public void GenerateKeySchema_PrimitiveAndComplex()`
- `public void GenerateNullablePrimitiveKeySchema_PrimitiveTypes()`
- `public void GeneratePrimitiveKeySchema_PrimitiveTypes()`
- `public void GenerateSchema_GeneratesValidSchema()`
- `public void GenerateTopicSchemas_ReturnsPair()`
- `public void GenerateTopicSchemas_WithName_UsesCustomName()`
- `public void GenerateValueSchema_UsesEntityType()`
- `public void GetAvroType_MapsSpecialTypes()`
- `public void GetGenerationStats_ReturnsCounts()`
- `public void GetIgnoredProperties_ReturnsIgnored()`
- `public void GetSerializableProperties_ExcludesIgnored()`
- `public void IsNullableProperty_DetectsNullableCorrectly()`
- `public void IsPrimitiveType_KnownTypes()`
- `public void MapPropertyToAvroType_HandlesNullable()`
- `public void SerializeSchema_WorksWithOptions()`
- `public void ToPascalCase_Works_WithVariousDelimiters()`
- `public void ValidateSchema_WorksForVariousInputs()`

### WindowBuilderTests

- `public void Build_NullExpression_ThrowsArgumentNullException()`
- `public void Build_TumblingWindowWithFinal_ReturnsClause()`

### WindowDataImporter

- `public async Task ImportFromCsv(CsvImportConfig config)`
- `public async Task ImportFromDatabase(DatabaseImportConfig config)`
- `public async Task ImportFromDirectory(DirectoryImportConfig config)`
- `public async Task ImportFromJson(JsonImportConfig config)`
- `public void Dispose()`

### WindowDef

- `public WindowDef AdvanceBy(TimeSpan ts)`
- `public WindowDef AdvanceBy(TimeSpan ts) => this`
- `public WindowDef EmitFinal()`
- `public WindowDef EmitFinal() => this`
- `public WindowDef Gap(TimeSpan ts)`
- `public WindowDef Gap(TimeSpan ts) => this`
- `public WindowDef GracePeriod(TimeSpan ts)`
- `public WindowDef GracePeriod(TimeSpan ts) => this`
- `public WindowDef HoppingWindow()`
- `public WindowDef HoppingWindow() => this`
- `public WindowDef Retention(TimeSpan ts)`
- `public WindowDef Retention(TimeSpan ts) => this`
- `public WindowDef SessionWindow()`
- `public WindowDef SessionWindow() => this`
- `public WindowDef Size(TimeSpan ts)`
- `public WindowDef Size(TimeSpan ts) => this`
- `public WindowDef TumblingWindow()`
- `public WindowDef TumblingWindow() => this`

### WindowDslTests

- `public FakeExecutor() : base(new NullLoggerFactory()) { }`
- `public override System.Threading.Tasks.Task ExecuteDDLAsync(string ddlQuery) => System.Threading.Tasks.Task.CompletedTask`
- `public override System.Threading.Tasks.Task StopAllQueriesAsync() => System.Threading.Tasks.Task.CompletedTask`
- `public override void Dispose() { }`
- `public override void ExecuteDDL(string ddlQuery) { }`
- `public void GenerateCreateTableAs_WithWindowAndGroupBy()`
- `public void GenerateKsqlQuery_WindowGroupBySelect_PushQuery()`

### WindowExtensions

- `public static IWindowCollection<T> Windows<T>(this IEntitySet<T> entitySet, params int[] windowSizes)`
- `public static IWindowedEntitySet<T> Window<T>(this IEntitySet<T> entitySet, int windowMinutes)`
- `public static IWindowedEntitySet<T> UseFinalized<T>(this IWindowedEntitySet<T> windowSet)`

### WindowFinalizationManager

- `public void Dispose()`
- `public void RegisterWindowProcessor<T>(WindowConfiguration<T> config) where T : class`

### WindowInfoExtensions

- `public static DateTime WindowEnd<TSource, TKey>(this IGrouping<TKey, TSource> source)`
- `public static DateTime WindowStart<TSource, TKey>(this IGrouping<TKey, TSource> source)`

### WindowModelBuilderExtensions

- `public static EntityModelBuilder<T> Window<T, TKey>(`
- `public static EntityModelBuilder<T> Window<T>(`

### WindowStartEndTests

- `public void ProjectionBuilder_WindowStartEnd_GeneratesFunctions()`
