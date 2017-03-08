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
			int additionCount = 0;

			wrappedConnectionList = transactionConnections.GetOrAdd(currentTransaction, GetNewConnectionCache(ref additionCount));
			if (additionCount > 0)
			{
				currentTransaction.TransactionCompleted += OnTransactionCompleted;
			}

			lock (wrappedConnectionList)
			{
				if (!wrappedConnectionList.TryGetValue(db.connectionString, out wrappedConnection))
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

			lock (connectionList)
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
		static Dictionary<string, ConnectionWrapper> GetNewConnectionCache(ref int count)
		{
			Interlocked.Increment(ref count);
			return new Dictionary<string, ConnectionWrapper>(1, Util.FastStringComparer.Instance);
		}// GetNewConnectionCache()
	}// class ConnectionCache
}//ns