using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SecurityDriven.TinyORM.Utils
{
	public static class ReflectionHelper_Shared
	{
		static Type ObjectType = typeof(object);
		static readonly ConstantExpression s_DbNullValue = Expression.Constant(DBNull.Value);

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

		static ConstructorInfo s_ValueTupleCtor = typeof(ValueTuple<object, Type>).GetConstructors()[0];
		internal static Dictionary<string, Func<T, (object, Type)>> GetPropertyGetters<T>(Type type, string prefix = "") where T : class
		{
			var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
			var dictionary = new Dictionary<string, Func<T, (object, Type)>>(source.Length, Util.FastStringComparer.Instance);

			var pInstance = Expression.Parameter(type);
			var parameterExpressionArray = new[] { pInstance };

			var expressionArrayOf2 = new Expression[2];
			ref var expressionArrayOf2_0 = ref expressionArrayOf2[0];
			ref var expressionArrayOf2_1 = ref expressionArrayOf2[1];

			ref var _ValueTupleCtor = ref s_ValueTupleCtor;
			ref var _ObjectType = ref ObjectType;

			foreach (PropertyInfo p in source)
			{
				string pName = p.Name;
				MethodInfo getMethod = p.GetGetMethod(nonPublic: true);
				if ((getMethod != null) && (pName != "Item" || getMethod.GetParameters().Length == 0))
				{
					UnaryExpression body = Expression.Convert(Expression.Call(pInstance, getMethod), _ObjectType);
					expressionArrayOf2_0 = body;
					expressionArrayOf2_1 = Expression.Constant(p.PropertyType);
					var valueTupleBody = Expression.New(_ValueTupleCtor, expressionArrayOf2);
					dictionary[prefix + pName] = Expression.Lambda<Func<T, (object, Type)>>(valueTupleBody, parameterExpressionArray).Compile();
				}
			}//foreach PropertyInfo

			return dictionary;
		}// GetPropertyGetters<T>()

		internal static Dictionary<string, Action<T, object>> GetPropertySetters<T>(Type type) where T : class
		{
			var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
			var dictionary = new Dictionary<string, Action<T, object>>(source.Length, Util.FastStringComparer.Instance);

			var valueCast_container = new Expression[1];
			ref var valueCast_container_0 = ref valueCast_container[0];

			var pInstance = Expression.Parameter(type, "instance");
			var pValue = Expression.Parameter(ObjectType, "value");
			var instance_and_value_container = new[] { pInstance, pValue };

			foreach (PropertyInfo p in source)
			{
				string pName = p.Name;
				Type propertyType = p.PropertyType;
				MethodInfo setMethod = p.GetSetMethod(nonPublic: true);
				if ((setMethod != null) && (pName != "Item" || setMethod.GetParameters().Length == 1))
				{
					valueCast_container_0 = Expression.Condition(
						Expression.Equal(pValue, s_DbNullValue), // test
						Expression.Default(propertyType), // if true
						Expression.Convert(pValue, propertyType)); // if false

					var setter = Expression.Lambda<Action<T, object>>(Expression.Call(pInstance, setMethod, valueCast_container), instance_and_value_container).Compile();
					dictionary[pName] = setter;
				}
			}//foreach PropertyInfo

			return dictionary;
		}// GetPropertySetters<T>()
	}// class ReflectionHelper_Shared
}//ns