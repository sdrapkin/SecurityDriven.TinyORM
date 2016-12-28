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

	using QueryInfoTuple = Tuple<string, Dictionary<string, object>>;

	public class DbContext
	{
		internal readonly string connectionString;
		public string ConnectionString => this.connectionString;

		internal DbContext(string connectionString)
		{
			this.connectionString = connectionString;
		}//ctor

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static DbContext CreateDbContext(string connectionString) => new DbContext(connectionString);

		#region QueryAsync()
		public async Task<List<dynamic>> QueryAsync<TParamType>(
			string sql,
			TParamType param,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = new CancellationToken(),
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class
		{
			var query = (await InternalQueryAsync(sql: sql, param: param, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber).ConfigureAwait(false))[0];
			return query;
		}// QueryAsync<TParamType>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<dynamic>> QueryAsync(
			string sql,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = new CancellationToken(),
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.QueryAsync<string>(sql: sql, param: null, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryAsync() - parameterless

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<dynamic>> QueryAsync(
			QueryInfoTuple queryInfo,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = new CancellationToken(),
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.QueryAsync(sql: queryInfo.Item1, param: queryInfo.Item2, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryAsync() - QueryInfoTuple
		#endregion

		#region QueryMultipleAsync
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<List<dynamic>>> QueryMultipleAsync<TParamType>(
			string sql,
			TParamType param,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = new CancellationToken(),
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class
		{
			return this.InternalQueryAsync(sql: sql, param: param, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryMultipleAsync<TParamType>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<List<dynamic>>> QueryMultipleAsync(
			string sql,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = new CancellationToken(),
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.QueryMultipleAsync<string>(sql: sql, param: null, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryMultipleAsync() -- parameterless

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Task<List<List<dynamic>>> QueryMultipleAsync(
			QueryInfoTuple queryInfo,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = new CancellationToken(),
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			return this.QueryMultipleAsync(sql: queryInfo.Item1, param: queryInfo.Item2, commandTimeout: commandTimeout, sqlTextOnly: sqlTextOnly, cancellationToken: cancellationToken, callerMemberName: callerMemberName, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
		}// QueryMultipleAsync() - QueryInfoTuple
		#endregion

		#region CommitUnitOfWorkAsync()

		public Task<int> CommitUnitOfWorkAsync(
			UnitOfWork uow,
			int batchSize = DBConstants.DEFAULT_BATCH_SIZE,
			CancellationToken cancellationToken = new CancellationToken(),
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		)
		{
			SqlCommand command;
			string commandString;
			int cumulativeResult = 0;
			int uowQueryCount = uow._queryList.Count;
			int index = -1;

			var callerIdentity = this.CallerIdentityDelegate();
			bool isAnonymous = callerIdentity.UserId == CallerIdentity.Anonymous.UserId;

			var batches = uow._queryList.GroupBy(_ =>
			{
				if (uowQueryCount - index - 1 > batchSize / 3)
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
								commandString = string.Concat(CommandExtensions.CMD_HEADER_UOW, element.Item1, CommandExtensions.CMD_FOOTER);
								command = new SqlCommand(commandString);
								if (element.Item2 != null)
								{
									command.SetupParameters(element.Item2, element.Item3);
								}

								command.SetupMetaParameters(callerIdentity.GetBytes(), callerMemberName, callerFilePath, callerLineNumber);
								sqlCommandSetWrapper.Append(command);
							}//element loop

							var conn = connWrapper.Connection;
							if (conn.State == ConnectionState.Closed)
							{
								// async statemachine overhead is not worth it for this call: SqlCommandSetWrapper.ExecuteNonQuery() is sync anyway
								// await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
								conn.Open();
							}
							sqlCommandSetWrapper.Connection = conn;

							sqlCommandSetWrapper.CommandTimeout = 0; // set infinite timeout for all sql commands in SqlCommandSet
							cumulativeResult += sqlCommandSetWrapper.ExecuteNonQuery();
						}// using SqlCommandSetWrapper
						if (cancellationToken.IsCancellationRequested) break;
					}//batch loop
				}//connWrapper
				ts.Complete();
			}//ts
			return Task.FromResult(cumulativeResult);
		}// CommitUnitOfWorkAsync()
		#endregion

		#region InternalQueryAsync
		async Task<List<List<dynamic>>> InternalQueryAsync<TParamType>(
			string sql,
			TParamType param,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			CancellationToken cancellationToken = new CancellationToken(),
			[CallerMemberName] string callerMemberName = null,
			[CallerFilePath] string callerFilePath = null,
			[CallerLineNumber] int callerLineNumber = 0
		) where TParamType : class
		{
			using (var ts = DbContext.CreateTransactionScope())
			{
				using (var connWrapper = this.GetWrappedConnection())
				{
					var conn = connWrapper.Connection;
					if (conn.State == ConnectionState.Closed)
						await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

					using (var comm = conn.CreateCommand())
					{
						comm.Setup(sql, param, CallerIdentityDelegate().GetBytes(), commandTimeout, sqlTextOnly, callerMemberName, callerFilePath, callerLineNumber);

						using (var reader = await comm.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
						{
							var result = await FetchResultSets(reader, cancellationToken).ConfigureAwait(false);
							ts.Complete();
							return result;
						}//reader
					}//comm
				}//connWrapper
			}//ts
		}// InternalQueryAsync<TParamType>()
		#endregion

		#region FetchResultSets
		internal static async Task<List<List<dynamic>>> FetchResultSets(SqlDataReader reader, CancellationToken cancellationToken = new CancellationToken())
		{
			var resultSetList = new List<List<dynamic>>(1); // optimizing for a single result set
			int resultSetId = 0, fieldCount = 0;
			object[] rowValues;
			bool canBeCancelled = cancellationToken.CanBeCanceled;

			do
			{
				var rowStoreList = new List<dynamic>(4);
				ResultSetSchema resultSchema = null;
				while (!(canBeCancelled && cancellationToken.IsCancellationRequested) && reader.Read()/*await reader.ReadAsync(cancellationToken).ConfigureAwait(false) */)
				{
					if (resultSchema == null)
					{
						fieldCount = reader.FieldCount;
						var fieldMap = new Dictionary<string, int>(fieldCount, Util.FastStringComparer.Instance);
						for (int i = 0; i < fieldCount; ++i)
						{
							fieldMap.Add(reader.GetName(i), i);
						}
						resultSchema = new ResultSetSchema(resultSetId, fieldMap);
					}

					rowValues = new object[fieldCount];
					reader.GetValues(rowValues);
					rowStoreList.Add(new RowStore(resultSchema, rowValues));
				}

				++resultSetId;
				resultSetList.Add(rowStoreList);
			} while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
			return resultSetList;
		}// FetchResultSets()
		#endregion

		#region CreateTransactionScope()

		static readonly TransactionOptions defaultTransactionOptions = new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted, Timeout = TimeSpan.FromSeconds(90) };

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
		public static TransactionScope CreateTransactionScope(TransactionScopeOption scopeOption, TransactionOptions transactionOptions)
		{
			var current_ts = Transaction.Current;
			if (current_ts == null)
				return new TransactionScope(scopeOption, transactionOptions, TransactionScopeAsyncFlowOption.Enabled);
			else
				return new TransactionScope(scopeOption, new TransactionOptions { IsolationLevel = current_ts.IsolationLevel }, TransactionScopeAsyncFlowOption.Enabled);
		}//CreateTransactionScope()
		#endregion

		#region CreateUnitOfWork
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static UnitOfWork CreateUnitOfWork() => new UnitOfWork();
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static UnitOfWork CreateUnitOfWork(IEnumerable<UnitOfWork> uowList) => new UnitOfWork().Append(uowList);
		#endregion

		#region CallerIdentity
		Func<CallerIdentity> callerIdentityDelegate = () => CallerIdentity.Anonymous;
		public Func<CallerIdentity> CallerIdentityDelegate => this.callerIdentityDelegate;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void SetCallerIdentityDelegate(Func<CallerIdentity> newCallerIdentityDelegate)
		{
			if (newCallerIdentityDelegate == null) throw new ArgumentNullException("callerIdentityDelegate");
			System.Threading.Interlocked.CompareExchange(ref this.callerIdentityDelegate, newCallerIdentityDelegate, this.callerIdentityDelegate);
		}// SetCallerIdentityDelegate()
		#endregion

		#region Connection-related
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		ConnectionWrapper GetWrappedConnection() => ConnectionCache.GetTransactionLinkedConnection(this) ?? this.GetNewWrappedConnection();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal ConnectionWrapper GetNewWrappedConnection() => new ConnectionWrapper(CreateNewConnection());

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		SqlConnection CreateNewConnection() => new SqlConnection(this.connectionString);
		#endregion

	}// class DbContext

	static class DBConstants
	{
		internal const int DEFAULT_BATCH_SIZE = 50;
	}// class DBConstants

	public enum StringType
	{
		CHAR = DbType.AnsiStringFixedLength,
		NCHAR = DbType.StringFixedLength,
		VARCHAR = DbType.AnsiString,
		NVARCHAR = DbType.String
	}// enum StringType
}//ns