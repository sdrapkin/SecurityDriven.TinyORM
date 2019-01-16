using System;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;
using System.Threading;

namespace SecurityDriven.TinyORM
{
	internal sealed class ConnectionWrapper : IDisposable
	{
		public SqlConnection Connection;
		int refCount = 1;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ConnectionWrapper(SqlConnection connection) { this.Connection = connection; }//ctor

		#region IDisposable Members
		/// <summary>Decrement the reference count and, if refcount is 0, close the underlying connection.</summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Dispose()
		{
			int count = Interlocked.Decrement(ref refCount);
			if (count == 0)
			{
				this.Connection.Close();
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