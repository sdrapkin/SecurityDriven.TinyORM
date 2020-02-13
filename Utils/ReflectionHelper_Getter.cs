using System;
using System.Collections.Generic;

namespace SecurityDriven.TinyORM.Utils
{
	public static class ReflectionHelper_Getter<T> where T : class
	{
		public static Dictionary<string, Func<T, (object, Type)>> Getters = ReflectionHelper_Shared.GetPropertyGetters<T>(type: typeof(T));
	}// class ReflectionHelper_Getter<T>
}//ns