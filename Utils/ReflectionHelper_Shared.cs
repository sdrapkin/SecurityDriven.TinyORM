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


		public static Dictionary<string, (object, Type)> ObjectToDictionary<T>(T obj) where T : class
		{
			var getters = ReflectionHelper_Getter<T>.Getters;
			var dictionary = new Dictionary<string, (object, Type)>(getters.Count, Util.FastStringComparer.Instance);
			var gettersEnumerator = getters.GetEnumerator();
			while (gettersEnumerator.MoveNext())
			{
				var kv = gettersEnumerator.Current;
				dictionary.Add(kv.Key, kv.Value(obj));
			}
			return dictionary;
		}// ObjectToDictionary<T>

		public static Dictionary<string, (object, Type)> ObjectToDictionary_Parameterized<T>(T obj) where T : class
		{
			var getters = ReflectionHelper_ParameterizedGetter<T>.Getters;
			var dictionary = new Dictionary<string, (object, Type)>(getters.Count, Util.FastStringComparer.Instance);
			var gettersEnumerator = getters.GetEnumerator();
			while (gettersEnumerator.MoveNext())
			{
				var kv = gettersEnumerator.Current;
				dictionary.Add(kv.Key, kv.Value(obj));
			}
			return dictionary;
		}// ObjectToDictionary_Parameterized<T>

		static ConstructorInfo _ValueTupleCtor = typeof(ValueTuple<object, Type>).GetConstructors()[0];
		internal static Dictionary<string, Func<T, (object, Type)>> GetPropertyGetters<T>(Type type, string prefix = "") where T : class
		{
			var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
			var dictionary = new Dictionary<string, Func<T, (object, Type)>>(source.Length, Util.FastStringComparer.Instance);

			var pInstance = Expression.Parameter(type);
			var parameterExpressionArray = new[] { pInstance };
			var expressionArrayOf2 = new Expression[2];
			foreach (PropertyInfo p in source)
			{
				if (p.GetIndexParameters().Length != 0) continue;
				MethodInfo getMethod = p.GetGetMethod(nonPublic: true);
				if (getMethod != null)
				{
					UnaryExpression body = Expression.Convert(Expression.Call(pInstance, getMethod), ObjectType);
					expressionArrayOf2[0] = body;
					expressionArrayOf2[1] = Expression.Constant(p.PropertyType);
					var valueTupleBody = Expression.New(_ValueTupleCtor, expressionArrayOf2);
					dictionary[prefix + p.Name] = Expression.Lambda<Func<T, (object, Type)>>(valueTupleBody, parameterExpressionArray).Compile();
				}
			}//foreach PropertyInfo

			return dictionary;
		}// GetPropertyGetters<T>()

		internal static Dictionary<string, Action<T, object>> GetPropertySetters<T>(Type type) where T : class
		{
			var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
			var dictionary = new Dictionary<string, Action<T, object>>(source.Length, Util.FastStringComparer.Instance);

			var valueCast_container = new UnaryExpression[1];

			var pInstance = Expression.Parameter(type, "instance");
			var pValue = Expression.Parameter(ObjectType, "value");
			var instance_and_value_container = new[] { pInstance, pValue };

			foreach (PropertyInfo p in source)
			{
				if (p.GetIndexParameters().Length != 0) continue;
				MethodInfo setMethod = p.GetSetMethod(nonPublic: true);
				if (setMethod != null)
				{
					var valueCast = Expression.Convert(pValue, p.PropertyType);
					valueCast_container[0] = valueCast;

					var setter = Expression.Lambda<Action<T, object>>(Expression.Call(pInstance, setMethod, valueCast_container), instance_and_value_container).Compile();
					dictionary[p.Name] = setter;
				}
			}//foreach PropertyInfo

			return dictionary;
		}// GetPropertySetters<T>()
	}// class ReflectionHelper_Shared
}//ns