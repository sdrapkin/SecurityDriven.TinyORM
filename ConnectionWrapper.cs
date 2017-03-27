using System;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;
using System.Threading;

namespace SecurityDriven.TinyORM
{
	internal class ConnectionWrapper : IDisposable
	{
		public SqlConnection Connection
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get { return connection; }
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			private set { this.connection = value; }
		}

		SqlConnection connection;
		int refCount = 1;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ConnectionWrapper(SqlConnection connection) { this.connection = connection; }//ctor

		#region IDisposable Members
		/// <summary>Decrement the reference count and, if refcount is 0, close the underlying connection.</summary>
		public void Dispose()
		{
			int count = Interlocked.Decrement(ref refCount);
			if (count == 0)
			{
				try
				{
					this.connection.Dispose();
				}
				finally
				{
					this.connection = null;
				}
			}
		}//Dispose()
		#endregion

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void IncrementUseCount()
		{
			Interlocked.Increment(ref refCount);
		}//AddRef()
	}//class ConnectionWrapper
}//ns