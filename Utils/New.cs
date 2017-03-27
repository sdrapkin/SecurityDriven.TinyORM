using System;
using System.Linq.Expressions;

namespace SecurityDriven.TinyORM.Utils
{
	internal static class New<T> where T : new() // very fast object/struct factory
	{
		public static Func<T> Instance = Expression.Lambda<Func<T>>(Expression.New(typeof(T)), Util.ZeroLengthArray<ParameterExpression>.Value).Compile();
	}// class New<T>
}//ns