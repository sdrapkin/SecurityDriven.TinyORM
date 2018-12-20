using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Transactions;

namespace SecurityDriven.TinyORM
{
	using Utils;

	internal static class ConnectionCache
	{
		public static readonly ConcurrentDictionary<Transaction, ConnectionWrapperContainer> transactionConnections = new ConcurrentDictionary<Transaction, ConnectionWrapperContainer>();

		public static ConnectionWrapper GetTransactionLinkedConnection(DbContext db)
		{
			Transaction currentTransaction = Transaction.Current;

			if (currentTransaction == null) return null;
			if (currentTransaction.TransactionInformation.Status == TransactionStatus.Aborted)
			{
				throw new TransactionAbortedException(nameof(GetTransactionLinkedConnection) + "() called on an already-aborted transaction.");
			}

			ConnectionWrapper wrappedConnection = null;
			ref readonly var _connectionString = ref db.connectionString;
			var connectionWrapperContainer = transactionConnections.GetOrAdd(key: currentTransaction, valueFactory: _getNewConnectionCache);

			lock (connectionWrapperContainer)
			{
				ref var _containerConnectionString = ref connectionWrapperContainer.ConnectionString;
				ref var _containerConnectionWrapper = ref connectionWrapperContainer.ConnectionWrapper;
				while (true)
				{
					if (_containerConnectionString == null)
					{
						_containerConnectionString = _connectionString;
						wrappedConnection = db.GetNewWrappedConnection();
						_containerConnectionWrapper = wrappedConnection;
						currentTransaction.TransactionCompleted += OnTransactionCompleted;
						break;
					}

					ref var _containerConnectionWrapperDictionary = ref connectionWrapperContainer.ConnectionWrapperDictionary;
					if (_containerConnectionWrapperDictionary == null)
					{
						if (_containerConnectionString == _connectionString)
						{
							System.Diagnostics.Debug.Assert(_containerConnectionWrapper != null);
							wrappedConnection = _containerConnectionWrapper;
						}
						else
						{
							// Add a 2nd ConnectionWrapper
							wrappedConnection = db.GetNewWrappedConnection();
							_containerConnectionWrapperDictionary = new Dictionary<string, ConnectionWrapper>(capacity: 2)
							{
								{ _containerConnectionString, _containerConnectionWrapper },
								{ _connectionString, wrappedConnection }
							};
							_containerConnectionWrapper = null;
						}
						break;
					}

					if (!_containerConnectionWrapperDictionary.TryGetValue(_connectionString, out wrappedConnection))
					{
						// Add 3rd or more ConnectionWrapper
						wrappedConnection = db.GetNewWrappedConnection();
						_containerConnectionWrapperDictionary.Add(_connectionString, wrappedConnection);
					}
					break;
				}// while (true)
			}// release lock

			wrappedConnection.IncrementUseCount();
			return wrappedConnection;
		}// GetTransactionLinkedConnection()

		static void OnTransactionCompleted(object sender, TransactionEventArgs e)
		{
			if (!transactionConnections.TryRemove(e.Transaction, out var connectionWrapperContainer)) return; // should never happen

			while (true)
			{
				ref var _containerConnectionWrapper = ref connectionWrapperContainer.ConnectionWrapper;
				if (_containerConnectionWrapper != null)
				{
					_containerConnectionWrapper.Dispose();
					break;
				}

				ref var _containerConnectionWrapperDictionary = ref connectionWrapperContainer.ConnectionWrapperDictionary;
				if (_containerConnectionWrapperDictionary != null)
				{
					var enumerator = _containerConnectionWrapperDictionary.GetEnumerator();
					while (enumerator.MoveNext())
					{
						try
						{
							enumerator.Current.Value.Dispose();
						}
						catch { }
					}// while enumerator
					break;
				}
			}// while (true)
		}// OnTransactionCompleted()

		static Func<Transaction, ConnectionWrapperContainer> _getNewConnectionCache = transaction => new ConnectionWrapperContainer();

		internal class ConnectionWrapperContainer
		{
			public string ConnectionString;
			public ConnectionWrapper ConnectionWrapper;
			public Dictionary<string, ConnectionWrapper> ConnectionWrapperDictionary;
		}// class ConnectionWrapperContainer
	}// class ConnectionCache
}//ns