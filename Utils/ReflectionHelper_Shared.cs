using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SecurityDriven.TinyORM.Utils
{
	public static class ReflectionHelper_Shared
	{
		public static readonly Type ObjectType = typeof(object);
		public const BindingFlags propertyBindingFlags = (BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
		public const string PARAM_PREFIX = "@";

		public static Dictionary<string, object> ObjectToDictionary<T>(T obj) where T : class
		{
			var getters = ReflectionHelper_Getter<T>.Getters;
			var dictionary = new Dictionary<string, object>(getters.Count, Util.FastStringComparer.Instance);
			var gettersEnumerator = getters.GetEnumerator();
			while (gettersEnumerator.MoveNext())
			{
				var kv = gettersEnumerator.Current;
				dictionary.Add(kv.Key, kv.Value(obj));
			}
			return dictionary;
		}// ObjectToDictionary<T>

		public static Dictionary<string, object> ObjectToDictionary<T>(T obj, string prefix) where T : class
		{
			var getters = ReflectionHelper_Getter<T>.Getters;
			var dictionary = new Dictionary<string, object>(getters.Count, Util.FastStringComparer.Instance);
			var gettersEnumerator = getters.GetEnumerator();
			while (gettersEnumerator.MoveNext())
			{
				var kv = gettersEnumerator.Current;
				dictionary.Add(prefix + kv.Key, kv.Value(obj));
			}
			return dictionary;
		}// ObjectToDictionary<T>

		public static Dictionary<string, Func<T, object>> GetPropertyGetters<T>(Type type, string prefix = "") where T : class
		{
			var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
			var dictionary = new Dictionary<string, Func<T, object>>(source.Length, Util.FastStringComparer.Instance);

			var pInstance = Expression.Parameter(ObjectType, "instance");
			var parameterExpressionArray = new[] { pInstance };
			foreach (PropertyInfo p in source)
			{
				if (p.GetIndexParameters().Length != 0) continue;
				MethodInfo getMethod = p.GetGetMethod(nonPublic: true);
				if (getMethod != null)
				{
					UnaryExpression body = Expression.Convert(Expression.Call(Expression.Convert(pInstance, type), getMethod), ObjectType);
					dictionary[prefix + p.Name] = Expression.Lambda<Func<T, object>>(body, parameterExpressionArray).Compile();
				}
			}//foreach PropertyInfo

			return dictionary;
		}// GetPropertyGetters<T>()

		public static Dictionary<string, Action<T, object>> GetPropertySetters<T>(Type type) where T : class
		{
			var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
			var dictionary = new Dictionary<string, Action<T, object>>(source.Length, Util.FastStringComparer.Instance);

			Expression[] valueCast_container = new Expression[1];

			var pValue = Expression.Parameter(ObjectType, "value");
			var pInstance = Expression.Parameter(ObjectType, "instance");
			var instance_and_value_container = new[] { pInstance, pValue };
			UnaryExpression instanceCast = Expression.Convert(pInstance, type);

			foreach (PropertyInfo p in source)
			{
				if (p.GetIndexParameters().Length != 0) continue;
				MethodInfo setMethod = p.GetSetMethod(nonPublic: true);
				if (setMethod != null)
				{
					UnaryExpression valueCast = Expression.Convert(pValue, p.PropertyType);
					valueCast_container[0] = valueCast;

					var setter = Expression.Lambda<Action<T, object>>(Expression.Call(instanceCast, setMethod, valueCast_container), instance_and_value_container).Compile();
					dictionary[p.Name] = setter;
				}
			}//foreach PropertyInfo

			return dictionary;
		}// GetPropertySetters<T>()
	}// class ReflectionHelper_Shared
}//ns