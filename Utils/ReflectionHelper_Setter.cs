using System;
using System.Collections.Generic;

namespace SecurityDriven.TinyORM.Utils
{
	public static class ReflectionHelper_Setter<T> where T : class
	{
		static readonly Type _typeofT = typeof(T);
		public static readonly Dictionary<string, Action<T, object>> Setters = ReflectionHelper_Shared.GetPropertySetters<T>(type: _typeofT);
	}// class ReflectionHelper_Setter<T>
}//ns