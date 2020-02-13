using System;
using System.Linq.Expressions;

namespace SecurityDriven.TinyORM.Utils
{
	internal static class New<T> where T : new() // very fast object/struct factory
	{
		public static readonly Func<T> Instance = ((Expression<Func<T>>)(() => new T())).Compile();
	}//class New<T>
}//ns