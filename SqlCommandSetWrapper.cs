using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq.Expressions;

namespace SecurityDriven.TinyORM
{
	internal class SqlCommandSetWrapper : IDisposable
	{
		static readonly Type commandSetType;
		static readonly Func<object> commandSetCtor;
		readonly object commandSetInstance;
		readonly Action<SqlCommand> appendDelegate;
		readonly Action disposeDelegate;
		readonly Func<int> executeNonQueryDelegate;
		readonly Func<SqlConnection> connectionGetDelegate;
		readonly Action<SqlConnection> connectionSetDelegate;
		readonly Action<int> commandTimeoutSetDelegate;

		int commandCount;

		static SqlCommandSetWrapper()
		{
			commandSetType = typeof(SqlCommand).Assembly.GetType("System.Data.SqlClient.SqlCommandSet");
			Debug.Assert(commandSetType != null, @"Could not find [System.Data.SqlClient.SqlCommandSet].");
			commandSetCtor = Expression.Lambda<Func<object>>(Expression.New(commandSetType)).Compile();
		}//static ctor

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