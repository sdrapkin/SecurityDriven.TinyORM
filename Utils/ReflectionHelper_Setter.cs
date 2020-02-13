using System;
using System.Collections.Generic;

namespace SecurityDriven.TinyORM.Utils
{
	public static class ReflectionHelper_Setter<T> where T : class
	{
		public static Dictionary<string, Action<T, object>> Setters = ReflectionHelper_Shared.GetPropertySetters<T>(type: typeof(T));
	}// class ReflectionHelper_Setter<T>
}//ns