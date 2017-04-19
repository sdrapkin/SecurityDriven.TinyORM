using System;
using System.Collections.Generic;

namespace SecurityDriven.TinyORM.Utils
{
	public static class ReflectionHelper_ParameterizedGetter<T> where T : class
	{
		static readonly Type _typeofT = typeof(T);
		public static readonly Dictionary<string, Func<T, object>> Getters = ReflectionHelper_Shared.GetPropertyGetters<T>(type: _typeofT, prefix: ReflectionHelper_Shared.PARAM_PREFIX);
	}// class ReflectionHelper_ParameterizedGetter<T>
}//ns