using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace SecurityDriven.TinyORM
{
	using Extensions;
	using Utils;

	public sealed class DbContext
	{
		internal readonly string connectionString;

		public string ConnectionString { [MethodImpl(MethodImplOptions.AggressiveInlining)] get { return this.connectionString; } }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal DbContext(string connectionString)
		{
			this.connectionString = connectionString;
		}//ctor

		static DbContext()
		{
			ThreadPool.GetMinThreads(out var minWorkerThreads, out var minCompletionPortThreads);
			int desiredMinWorkerThreads = Environment.ProcessorCount << 1;
			if (minWorkerThreads < desiredMinWorkerThreads) ThreadPool.SetMinThreads(desiredMinWorkerThreads, minCompletionPortThreads);
		}// static ctor

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static DbContext Create(string connectionString) => new DbContext(connectionString);

		#region QueryAsync()
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<RowStore>> QueryAsync<TParamType>(
			string sql,
			TParamType param,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class
		{
			var query = InternalQueryAsync(sql: sql, param: param, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
			return query;
		}// QueryAsync<TParamType>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<RowStore>> QueryAsync(
			string sql,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.QueryAsync<string>(sql: sql, param: null, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryAsync() - parameterless

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<RowStore>> QueryAsync(
			QueryInfo queryInfo,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.QueryAsync(sql: queryInfo.SQL, param: queryInfo.ParameterMap, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryAsync() - QueryInfo

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<T>> QueryAsync<TParamType, T>(
			string sql,
			TParamType param,
			Func<T> entityFactory,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class where T : class, new()
		{
			return InternalQueryAsync<TParamType, T>(sql, param, entityFactory, commandTimeout, sqlTextOnly, cancellationToken, callerMemberName, callerFilePath, callerLineNumber);
		}// QueryAsync<TParamType, T>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<T>> QueryAsync<T>(
			string sql,
			Func<T> entityFactory,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where T : class, new()
		{
			return InternalQueryAsync<string, T>(sql, null, entityFactory, commandTimeout, sqlTextOnly, cancellationToken, callerMemberName, callerFilePath, callerLineNumber);
		}// QueryAsync<T>() parameterless

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<T>> QueryAsync<T>(
			QueryInfo queryInfo,
			Func<T> entityFactory,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where T : class, new()
		{
			return InternalQueryAsync(queryInfo.SQL, queryInfo.ParameterMap, entityFactory, commandTimeout, sqlTextOnly, cancellationToken, callerMemberName, callerFilePath, callerLineNumber);
		}// QueryAsync<T>() - QueryInfo
		#endregion

		#region QueryMultipleAsync()
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<List<RowStore>>> QueryMultipleAsync<TParamType>(
			string sql,
			TParamType param,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class
		{
			return this.InternalQueryMultipleAsync(sql: sql, param: param, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryMultipleAsync<TParamType>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<List<RowStore>>> QueryMultipleAsync(
			string sql,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.QueryMultipleAsync<string>(sql: sql, param: null, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryMultipleAsync() -- parameterless

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<List<RowStore>>> QueryMultipleAsync(
			QueryInfo queryInfo,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.QueryMultipleAsync(sql: queryInfo.SQL, param: queryInfo.ParameterMap, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryMultipleAsync() - QueryInfo
		#endregion

		#region CommitQueryBatchAsync()

		public async Task<int> CommitQueryBatchAsync(
			QueryBatch queryBatch,
			int batchSize = 0,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			string commandString;
			int cumulativeResult = 0;
			int queryBatchCount = queryBatch.queryList.Count;
			int index = -1;

			if (batchSize == 0) batchSize = this.BatchSize;
			int shortBatchSize = batchSize / 3;

			var callerIdentity = callerIdentityDelegate();

			var batches = queryBatch.queryList.GroupBy(_ =>
			{
				if (queryBatchCount - index - 1 > shortBatchSize)
					++index;
				return index / batchSize;
			});

			using (var ts = DbContext.CreateTransactionScope())
			{
				using (var connWrapper = this.GetWrappedConnection())
				{
					var conn = connWrapper.Connection;

					foreach (var batch in batches)
					{
						if (conn.State != ConnectionState.Open) conn.Open();

						using (var sqlCommandSetWrapper = new SqlCommandSetWrapper())
						{
							foreach (var element in batch)
							{
								commandString = string.Concat(CommandExtensions.CMD_HEADER_QUERYBATCH, element.Item1, CommandExtensions.CMD_FOOTER);
								var command = new SqlCommand(commandString);
								if (element.Item2 != null)
								{
									command.SetParametersFromDictionary(element.Item2);
								}

								command.SetupMetaParameters(callerIdentity.UserIdAsBytes, callerMemberName, callerFilePath, callerLineNumber);
								sqlCommandSetWrapper.Append(command);
							}//foreach element loop

							sqlCommandSetWrapper.Connection = conn;
							sqlCommandSetWrapper.CommandTimeout = 0; // set infinite timeout for all sql commands in SqlCommandSet
							cumulativeResult += sqlCommandSetWrapper.ExecuteNonQuery();
						}// using SqlCommandSetWrapper
						if (cancellationToken.IsCancellationRequested) break;
					}//foreach batch loop
				}//connWrapper
				ts.Complete();
			}//ts
			return cumulativeResult;
		}// CommitQueryBatchAsync()
		#endregion

		#region InternalQueryMultipleAsync()
		async Task<List<List<RowStore>>> InternalQueryMultipleAsync<TParamType>(
			string sql,
			TParamType param,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class
		{
			using (var ts = (commandTimeout == null) ? DbContext.CreateTransactionScope() : DbContext.CreateTransactionScope(commandTimeout.Value))
			{
				var connWrapper = this.GetWrappedConnection();
				try
				{
					var conn = connWrapper.Connection;
					var comm = new SqlCommand(null, conn);
					comm.Setup(sql, param, callerIdentityDelegate().UserIdAsBytes, commandTimeout, sqlTextOnly, callerMemberName, callerFilePath, callerLineNumber);

					if (conn.State != ConnectionState.Open) conn.Open();
					var reader = await comm.ExecuteReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
					try
					{
						var result = FetchMultipleRowStoreResultSets(reader);
						ts.Complete();
						return result;
					}//reader-try
					finally
					{
						reader.Close();
					}
				}//connWrapper-try
				finally
				{
					connWrapper.Dispose();
				}
			}//ts
		}// InternalQueryMultipleAsync<TParamType>()
		#endregion

		#region InternalQueryAsync()
		async Task<List<RowStore>> InternalQueryAsync<TParamType>(
			string sql,
			TParamType param,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class
		{
			var ts = (commandTimeout == null) ? DbContext.CreateTransactionScope() : DbContext.CreateTransactionScope(commandTimeout.Value);
			try
			{
				var connWrapper = this.GetWrappedConnection();
				try
				{
					var conn = connWrapper.Connection;
					var comm = new SqlCommand(null, conn);
					comm.Setup(sql, param, callerIdentityDelegate().UserIdAsBytes, commandTimeout, sqlTextOnly, callerMemberName, callerFilePath, callerLineNumber);

					if (conn.State != ConnectionState.Open) conn.Open();
					var reader = await comm.ExecuteReaderAsync(CommandBehavior.SingleResult, cancellationToken).ConfigureAwait(false);
					try
					{
						var result = FetchSingleRowStoreResultSet(reader);
						ts.Complete();
						return result;
					}//reader-try
					finally
					{
						reader.Close();
					}
				}//connWrapper-try
				finally
				{
					connWrapper.Dispose();
				}
			}
			finally
			{
				ts.Dispose();
			}
		}// InternalQueryAsync<TParamType>()

		// entity-returning
		public async Task<List<T>> InternalQueryAsync<TParamType, T>(
			string sql,
			TParamType param,
			Func<T> entityFactory,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class where T : class, new()
		{
			var ts = (commandTimeout == null) ? DbContext.CreateTransactionScope() : DbContext.CreateTransactionScope(commandTimeout.Value);
			try
			{
				var connWrapper = this.GetWrappedConnection();
				try
				{
					var conn = connWrapper.Connection;
					var comm = new SqlCommand(null, conn);
					comm.Setup(sql, param, callerIdentityDelegate().UserIdAsBytes, commandTimeout, sqlTextOnly, callerMemberName, callerFilePath, callerLineNumber);

					if (conn.State != ConnectionState.Open) conn.Open();
					var reader = await comm.ExecuteReaderAsync(CommandBehavior.SingleResult, cancellationToken).ConfigureAwait(false);
					try
					{
						var result = entityFactory == null ? FetchSingleEntityResultSet<T>(reader, New<T>.Instance) : FetchSingleEntityResultSet<T>(reader, entityFactory);
						ts.Complete();
						return result;
					}//reader-try
					finally
					{
						reader.Close();
					}
				}//connWrapper-try
				finally
				{
					connWrapper.Dispose();
				}
			}
			finally
			{
				ts.Dispose();
			}
		}// InternalQueryAsync<TParamType>()
		#endregion

		#region FetchSingleEntityResultSet()
		internal static List<T> FetchSingleEntityResultSet<T>(SqlDataReader reader, Func<T> entityFactory) where T : class, new()
		{
			var rowStoreList = new List<T>(capacity: 8);

			unchecked
			{
				if (reader.Read())
				{
					var rowEntity = entityFactory();
					int i = reader.FieldCount;

					var rowValues = new object[i];
					reader.GetValues(rowValues);

					var setterArray = new Action<T, object>[i];
					var setters = ReflectionHelper_Setter<T>.Setters;
					for (i = 0; i < rowValues.Length; ++i)
					{
						var fieldName = reader.GetName(i);
						if (setters.TryGetValue(fieldName, out var setter))
						{
							setter(rowEntity, rowValues[i]);
							setterArray[i] = setter;
						}
					}

					rowStoreList.Add(rowEntity);

					while (reader.Read())
					{
						rowEntity = entityFactory();
						reader.GetValues(rowValues);

						for (i = 0; i < rowValues.Length; ++i)
						{
							setterArray[i]?.Invoke(rowEntity, rowValues[i]);
						}//for

						rowStoreList.Add(rowEntity);
					}
				}// if 1st row is read
			}// unchecked

			return rowStoreList;
		}// FetchSingleEntityResultSet()
		#endregion

		#region FetchMultipleRowStoreResultSets()
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static List<List<RowStore>> FetchMultipleRowStoreResultSets(SqlDataReader reader)
		{
			var resultSetList = new List<List<RowStore>>(4);
			unchecked
			{
				int resultSetId = 0;

				do
				{
					var rowStoreList = FetchSingleRowStoreResultSet(reader, resultSetId++);
					resultSetList.Add(rowStoreList);
				} while (reader.NextResult());
			}// unchecked
			return resultSetList;
		}// FetchMultipleRowStoreResultSets()
		#endregion

		#region FetchSingleRowStoreResultSet()
		internal static List<RowStore> FetchSingleRowStoreResultSet(SqlDataReader reader, int resultSetId = 0)
		{
			var rowStoreList = new List<RowStore>(capacity: 8);
			unchecked
			{
				if (reader.Read())
				{
					int fieldCount = reader.FieldCount;
					int fieldCountPlusOne = fieldCount + 1;
					var fieldMap = new Dictionary<string, int>(fieldCount, Util.FastStringComparer.Instance);
					var fieldNames = new string[fieldCount];
					for (int i = 0; i < fieldNames.Length; ++i)
					{
						var fieldName = reader.GetName(i);
						fieldMap.Add(fieldName, i);
						fieldNames[i] = fieldName;
					}
					var resultSchema = new ResultSetSchema(resultSetId, fieldMap, fieldNames);

					var rowValues = new object[fieldCountPlusOne];
					reader.GetValues(rowValues);
					rowValues[fieldCount] = resultSchema;
					rowStoreList.Add(new RowStore(rowValues));

					while (reader.Read())
					{
						rowValues = new object[fieldCountPlusOne];
						reader.GetValues(rowValues);
						rowValues[fieldCount] = resultSchema;
						rowStoreList.Add(new RowStore(rowValues));
					}
				}// if 1st read
			}// unchecked
			return rowStoreList;
		}// FetchSingleRowStoreResultSet()
		#endregion

		#region CreateTransactionScope()

		static TransactionOptions defaultTransactionOptions = new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted, Timeout = TimeSpan.FromSeconds(90) };

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		static TransactionScope CreateTransactionScope(int timeout)
		{
			var customTransactionOptions = new TransactionOptions { IsolationLevel = defaultTransactionOptions.IsolationLevel, Timeout = TimeSpan.FromSeconds(timeout) };
			return CreateTransactionScope(TransactionScopeOption.Required, customTransactionOptions);
		}// CreateTransactionScope(int timeout)

		/// <summary>
		/// Creates a new TransactionScope if none exists, or joins an existing one.
		/// </summary>
		/// <returns>A new TransactionScope.</returns>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static TransactionScope CreateTransactionScope() => CreateTransactionScope(TransactionScopeOption.Required, defaultTransactionOptions);

		/// <summary>
		/// Creates a new TransactionScope if none exists, or joins an existing one.
		/// </summary>
		/// <param name="scopeOption">An instance of the TransactionScopeOption enumeration that describes the transaction requirements associated with this transaction scope.</param>
		/// <returns>A new TransactionScope.</returns>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static TransactionScope CreateTransactionScope(TransactionScopeOption scopeOption) => CreateTransactionScope(scopeOption, defaultTransactionOptions);

		/// <summary>
		/// Creates a new TransactionScope if none exists, or joins an existing one.
		/// </summary>
		/// <param name="scopeOption">An instance of the TransactionScopeOption enumeration that describes the transaction requirements associated with this transaction scope.</param>
		/// <param name="transactionOptions">A TransactionOptions structure that describes the transaction options to use if a new transaction is created. If an existing transaction is used, the timeout value in this parameter applies to the transaction scope. If that time expires before the scope is disposed, the transaction is aborted.</param>
		/// <returns>A new TransactionScope.</returns>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static TransactionScope CreateTransactionScope(TransactionScopeOption scopeOption, TransactionOptions transactionOptions)
		{
			var current_ts = Transaction.Current;
			if (current_ts == null || scopeOption == TransactionScopeOption.RequiresNew)
				return new TransactionScope(scopeOption, transactionOptions, TransactionScopeAsyncFlowOption.Enabled);

			return new TransactionScope(scopeOption, new TransactionOptions { IsolationLevel = current_ts.IsolationLevel, Timeout = transactionOptions.Timeout }, TransactionScopeAsyncFlowOption.Enabled);
		}//CreateTransactionScope()
		#endregion

		#region CallerIdentity
		static readonly Func<CallerIdentity> anonymousCallerIdentityDelegate = () => CallerIdentity.Anonymous;
		Func<CallerIdentity> callerIdentityDelegate = anonymousCallerIdentityDelegate;
		public Func<CallerIdentity> CallerIdentityDelegate
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get { return this.callerIdentityDelegate; }

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			set
			{
				var newCallerIdentityDelegate = value;
				if (newCallerIdentityDelegate == null) ThrowArgumentNullException();
				Interlocked.CompareExchange(ref this.callerIdentityDelegate, newCallerIdentityDelegate, this.callerIdentityDelegate);

				void ThrowArgumentNullException() => throw new ArgumentNullException(nameof(newCallerIdentityDelegate));
			}
		}// CallerIdentityDelegate			
		#endregion

		#region Connection-related
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		ConnectionWrapper GetWrappedConnection() => ConnectionCache.GetTransactionLinkedConnection(this) ?? this.GetNewWrappedConnection();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal ConnectionWrapper GetNewWrappedConnection() => new ConnectionWrapper(new SqlConnection(this.connectionString, credential: null));
		#endregion

		#region SequentialReaderAsync()
		public async Task<bool> SequentialReaderAsync<TParamType>(
			string sql,
			TParamType param,
			Func<SqlDataReader, CancellationToken, Task<bool>> actionAsync,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class
		{
			bool result;
			using (var ts = (commandTimeout == null) ? DbContext.CreateTransactionScope() : DbContext.CreateTransactionScope(commandTimeout.Value))
			{
				using (var connWrapper = this.GetWrappedConnection())
				{
					var conn = connWrapper.Connection;
					var comm = new SqlCommand(null, conn);
					comm.Setup(sql, param, callerIdentityDelegate().UserIdAsBytes, commandTimeout, sqlTextOnly, callerMemberName, callerFilePath, callerLineNumber);

					if (conn.State != ConnectionState.Open) conn.Open();
					using (var reader = await comm.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
					{
						result = await actionAsync(reader, cancellationToken).ConfigureAwait(false);
					}//reader
					ts.Complete();
				}//connWrapper
			}//ts
			return result;
		}// SequentialReaderAsync<TParamType>()

		public Task<bool> SequentialReaderAsync(
			string sql,
			Func<SqlDataReader, CancellationToken, Task<bool>> actionAsync,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = default,
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.SequentialReaderAsync<string>(sql, null, actionAsync, commandTimeout, sqlTextOnly, cancellationToken, callerMemberName, callerFilePath, callerLineNumber);
		}// SequentialReaderAsync()
		#endregion

		const int DEFAULT_BATCH_SIZE = 50;
		public int BatchSize { get; set; } = DEFAULT_BATCH_SIZE;

	}// class DbContext

	public enum StringType
	{
		CHAR = DbType.AnsiStringFixedLength,
		NCHAR = DbType.StringFixedLength,
		VARCHAR = DbType.AnsiString,
		NVARCHAR = DbType.String
	}// enum StringType
}//ns