using System;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq.Expressions;

namespace SecurityDriven.TinyORM
{
	internal sealed class SqlCommandSetWrapper : IDisposable
	{
		static Type commandSetType = typeof(SqlCommand).Assembly.GetType("Microsoft.Data.SqlClient.SqlCommandSet")
			?? throw new Exception($"{nameof(SqlCommandSetWrapper)}: Could not find[Microsoft.Data.SqlClient.SqlCommandSet].");
		static Func<object> commandSetCtor = Expression.Lambda<Func<object>>(Expression.New(commandSetType)).Compile();
		object commandSetInstance;
		Action<SqlCommand> appendDelegate;
		Action disposeDelegate;
		Func<int> executeNonQueryDelegate;
		Func<SqlConnection> connectionGetDelegate;
		Action<SqlConnection> connectionSetDelegate;
		Action<int> commandTimeoutSetDelegate;

		int commandCount;

		public SqlCommandSetWrapper()
		{
			commandSetInstance = commandSetCtor();
			appendDelegate = (Action<SqlCommand>)Delegate.CreateDelegate(typeof(Action<SqlCommand>), commandSetInstance, "Append");
			disposeDelegate = (Action)Delegate.CreateDelegate(typeof(Action), commandSetInstance, "Dispose");
			executeNonQueryDelegate = (Func<int>)Delegate.CreateDelegate(typeof(Func<int>), commandSetInstance, "ExecuteNonQuery");
			connectionGetDelegate = (Func<SqlConnection>)Delegate.CreateDelegate(typeof(Func<SqlConnection>), commandSetInstance, "get_Connection");
			connectionSetDelegate = (Action<SqlConnection>)Delegate.CreateDelegate(typeof(Action<SqlConnection>), commandSetInstance, "set_Connection");

			commandTimeoutSetDelegate = (Action<int>)Delegate.CreateDelegate(typeof(Action<int>), commandSetInstance, "set_CommandTimeout");
		}//ctor

		public void Dispose() => disposeDelegate();
		public int CommandTimeout { set { commandTimeoutSetDelegate(value); } }
		public int CommandCount { get { return commandCount; } }

		public SqlConnection Connection
		{
			get { return connectionGetDelegate(); }
			set { connectionSetDelegate(value); }
		}// Connection

		public int ExecuteNonQuery() => executeNonQueryDelegate();

		public void Append(SqlCommand command)
		{
			appendDelegate(command);
			++commandCount;
		}// Append

	}//class SqlCommandSetWrapper
}//ns