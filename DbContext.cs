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

	public class DbContext
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
		public async ValueTask<IReadOnlyList<dynamic>> QueryAsync<TParamType>(
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
			var query = (await InternalQueryAsync(sql: sql, param: param, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber).ConfigureAwait(false))[0];
			return query;
		}// QueryAsync<TParamType>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ValueTask<IReadOnlyList<dynamic>> QueryAsync(
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
		public ValueTask<IReadOnlyList<dynamic>> QueryAsync(
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
		#endregion

		#region QueryMultipleAsync()
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ValueTask<IReadOnlyList<IReadOnlyList<dynamic>>> QueryMultipleAsync<TParamType>(
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
			return this.InternalQueryAsync(sql: sql, param: param, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryMultipleAsync<TParamType>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ValueTask<IReadOnlyList<IReadOnlyList<dynamic>>> QueryMultipleAsync(
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
		public ValueTask<IReadOnlyList<IReadOnlyList<dynamic>>> QueryMultipleAsync(
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

		public async ValueTask<int> CommitQueryBatchAsync(
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
					foreach (var batch in batches)
					{
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
							}//element loop

							var conn = connWrapper.Connection;
							if (conn.State != ConnectionState.Open) await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
							sqlCommandSetWrapper.Connection = conn;

							sqlCommandSetWrapper.CommandTimeout = 0; // set infinite timeout for all sql commands in SqlCommandSet
							cumulativeResult += sqlCommandSetWrapper.ExecuteNonQuery();
						}// using SqlCommandSetWrapper
						if (cancellationToken.IsCancellationRequested) break;
					}//batch loop
				}//connWrapper
				ts.Complete();
			}//ts
			return cumulativeResult;
		}// CommitQueryBatchAsync()
		#endregion

		#region InternalQueryAsync()
		async ValueTask<IReadOnlyList<IReadOnlyList<dynamic>>> InternalQueryAsync<TParamType>(
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
				using (var connWrapper = this.GetWrappedConnection())
				{
					var conn = connWrapper.Connection;
					var comm = new SqlCommand(null, conn);
					comm.Setup(sql, param, callerIdentityDelegate().UserIdAsBytes, commandTimeout, sqlTextOnly, callerMemberName, callerFilePath, callerLineNumber);

					if (conn.State != ConnectionState.Open) await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

					using (var reader = await comm.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
					{
						var result = FetchResultSets(reader);
						ts.Complete();
						return result;
					}//reader
				}//connWrapper
			}//ts
		}// InternalQueryAsync<TParamType>()
		#endregion

		#region FetchResultSets()
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static List<List<RowStore>> FetchResultSets(SqlDataReader reader)
		{
			var resultSetList = new List<List<RowStore>>(1); // optimizing for a single result set
			int resultSetId = 0, fieldCount = 0;

			do
			{
				var rowStoreList = new List<RowStore>(2);
				ResultSetSchema resultSchema = null;

				if (reader.Read())
				{
					fieldCount = reader.FieldCount;
					var fieldMap = new Dictionary<string, int>(fieldCount, Util.FastStringComparer.Instance);
					for (int i = 0; i < fieldCount; ++i)
					{
						fieldMap.Add(reader.GetName(i), i);
					}
					resultSchema = new ResultSetSchema(resultSetId, fieldMap);

					var rowValues = new object[fieldCount];
					reader.GetValues(rowValues);
					rowStoreList.Add(new RowStore(ref resultSchema, ref rowValues));
				}

				while (reader.Read())
				{
					var rowValues = new object[fieldCount];
					reader.GetValues(rowValues);
					rowStoreList.Add(new RowStore(ref resultSchema, ref rowValues));
				}

				++resultSetId;
				resultSetList.Add(rowStoreList);
			} while (reader.NextResult());
			return resultSetList;
		}// FetchResultSets()
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

			set
			{
				var newCallerIdentityDelegate = value;
				if (newCallerIdentityDelegate == null) throw new ArgumentNullException(nameof(newCallerIdentityDelegate));
				Interlocked.CompareExchange(ref this.callerIdentityDelegate, newCallerIdentityDelegate, this.callerIdentityDelegate);
			}
		}// CallerIdentityDelegate			
		#endregion

		#region Connection-related
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		ConnectionWrapper GetWrappedConnection() => ConnectionCache.GetTransactionLinkedConnection(this) ?? this.GetNewWrappedConnection();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal ConnectionWrapper GetNewWrappedConnection() => new ConnectionWrapper(CreateNewConnection());

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		SqlConnection CreateNewConnection() => new SqlConnection(this.connectionString, null);
		#endregion

		#region SequentialReaderAsync()
		public async ValueTask<bool> SequentialReaderAsync<TParamType>(
			string sql,
			TParamType param,
			Func<SqlDataReader, CancellationToken, ValueTask<bool>> actionAsync,
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

					if (conn.State != ConnectionState.Open) await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

					using (var reader = await comm.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
					{
						result = await actionAsync(reader, cancellationToken).ConfigureAwait(false);
					}//reader
					ts.Complete();
				}//connWrapper
			}//ts
			return result;
		}// SequentialReaderAsync<TParamType>()

		public ValueTask<bool> SequentialReaderAsync(
			string sql,
			Func<SqlDataReader, CancellationToken, ValueTask<bool>> actionAsync,
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