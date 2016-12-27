using System;
using System.Data.SqlClient;
using System.Threading;

namespace SecurityDriven.TinyORM
{
	internal class ConnectionWrapper : IDisposable
	{
		public SqlConnection Connection { get { return connection; } private set { this.connection = value; } }
		SqlConnection connection;
		int refCount = 1;

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

		public ConnectionWrapper AddRef()
		{
			Interlocked.Increment(ref refCount);
			return this;
		}//AddRef()
	}//class ConnectionWrapper
}//ns