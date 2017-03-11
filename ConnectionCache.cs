using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
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
				throw new TransactionAbortedException("GetTransactionLinkedConnection() called on an already-aborted transaction.");
			}

			Dictionary<string, ConnectionWrapper> wrappedConnectionList;
			ConnectionWrapper wrappedConnection;

			wrappedConnectionList = transactionConnections.GetOrAdd(key: currentTransaction, valueFactory: GetNewConnectionCache);

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
			return wrappedConnection.AddRef();
		}// GetTransactionLinkedConnection()

		static void OnTransactionCompleted(object sender, TransactionEventArgs e)
		{
			if (!transactionConnections.TryRemove(e.Transaction, out var connectionList))
			{
				return; // should never happen
			}

			//lock (connectionList)
			{
				foreach (var kvp in connectionList)
				{
					try
					{
						kvp.Value.Dispose();
					}
					catch { }
				}
			}//lock
		}// OnTransactionCompleted()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		static Dictionary<string, ConnectionWrapper> GetNewConnectionCache(Transaction transaction)
		{
			return new Dictionary<string, ConnectionWrapper>(capacity: 1, comparer: Util.FastStringComparer.Instance);
		}// GetNewConnectionCache()
	}// class ConnectionCache
}//ns