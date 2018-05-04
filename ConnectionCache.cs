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
		public static readonly ConcurrentDictionary<Transaction, Dictionary<string, ConnectionWrapper>> transactionConnections = new ConcurrentDictionary<Transaction, Dictionary<string, ConnectionWrapper>>();

		public static ConnectionWrapper GetTransactionLinkedConnection(DbContext db)
		{
			Transaction currentTransaction = Transaction.Current;

			if (currentTransaction == null) return null;
			if (currentTransaction.TransactionInformation.Status == TransactionStatus.Aborted)
			{
				throw new TransactionAbortedException(nameof(GetTransactionLinkedConnection) + "() called on an already-aborted transaction.");
			}

			ConnectionWrapper wrappedConnection;
			var wrappedConnectionList = transactionConnections.GetOrAdd(key: currentTransaction, valueFactory: _getNewConnectionCache);

			lock (wrappedConnectionList)
			{
				bool isEmpty = wrappedConnectionList.Count == 0;
				if (isEmpty)
				{
					currentTransaction.TransactionCompleted += OnTransactionCompleted;
				}

				if (isEmpty || !wrappedConnectionList.TryGetValue(db.connectionString, out wrappedConnection))
				{
					wrappedConnection = db.GetNewWrappedConnection();
					wrappedConnectionList.Add(db.connectionString, wrappedConnection);
				}
			}//lock

			wrappedConnection.IncrementUseCount();
			return wrappedConnection;
		}// GetTransactionLinkedConnection()

		static void OnTransactionCompleted(object sender, TransactionEventArgs e)
		{
			if (!transactionConnections.TryRemove(e.Transaction, out var connectionList))
			{
				return; // should never happen
			}

			//lock (connectionList)
			{
				var connectionListEnumerator = connectionList.GetEnumerator();
				while (connectionListEnumerator.MoveNext())
				{
					try
					{
						connectionListEnumerator.Current.Value.Dispose();
					}
					catch { }
				}
			}//lock
		}// OnTransactionCompleted()

		static Func<Transaction, Dictionary<string, ConnectionWrapper>> _getNewConnectionCache =
			transaction => new Dictionary<string, ConnectionWrapper>(capacity: 1, comparer: Util.FastStringComparer.Instance);
	}// class ConnectionCache
}//ns