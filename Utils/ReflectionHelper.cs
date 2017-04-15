using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SecurityDriven.TinyORM.Utils
{
	static class ReflectionHelper
	{
		static readonly Type ObjectType = typeof(object);
		const BindingFlags propertyBindingFlags = (BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

		#region Property Getters
		/*
		public static readonly ConcurrentDictionary<Type, Dictionary<string, Func<object, object>>> PropertyGetters = new ConcurrentDictionary<Type, Dictionary<string, Func<object, object>>>();
		public static Dictionary<string, Func<object, object>> GetPropertyGetters(Type type)
		{
			if (!PropertyGetters.TryGetValue(type, out var dictionary))
			{
				var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
				dictionary = new Dictionary<string, Func<object, object>>(source.Length, Util.FastStringComparer.Instance);
				var parameterExpressionArray = new ParameterExpression[1];
				foreach (PropertyInfo p in source)
				{
					if (p.GetIndexParameters().Length != 0) continue;
					MethodInfo getMethod = p.GetGetMethod(nonPublic: true);
					if (getMethod != null)
					{
						parameterExpressionArray[0] = Expression.Parameter(ObjectType, "instance");
						UnaryExpression body = Expression.Convert(Expression.Call(Expression.Convert(parameterExpressionArray[0], type), getMethod), ObjectType);
						dictionary["@" + p.Name] = Expression.Lambda<Func<object, object>>(body, parameterExpressionArray).Compile();
					}
				}
				PropertyGetters.TryAdd(type, dictionary);
			}
			return dictionary;
		}// GetPropertyGetters()
		*/
		#endregion

		#region Property Setters
		public static readonly ConcurrentDictionary<Type, Dictionary<string, Action<object, object>>> PropertySetters = new ConcurrentDictionary<Type, Dictionary<string, Action<object, object>>>();
		public static Dictionary<string, Action<object, object>> GetPropertySetters(Type type)
		{
			if (!PropertySetters.TryGetValue(type, out var dictionary))
			{
				var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
				dictionary = new Dictionary<string, Action<object, object>>(source.Length, Util.FastStringComparer.Instance);

				Expression[] valueCast_container = new Expression[1];
				ParameterExpression[] instance_and_value_container = new ParameterExpression[2];
				foreach (PropertyInfo p in source)
				{
					if (p.GetIndexParameters().Length != 0) continue;
					MethodInfo setMethod = p.GetSetMethod(nonPublic: true);
					if (setMethod != null)
					{
						var instance = Expression.Parameter(ObjectType, "instance");
						var value = Expression.Parameter(ObjectType, "value");

						UnaryExpression instanceCast = Expression.Convert(instance, type);
						UnaryExpression valueCast = Expression.Convert(value, p.PropertyType);

						valueCast_container[0] = valueCast;
						instance_and_value_container[0] = instance;
						instance_and_value_container[1] = value;
						var setter = Expression.Lambda<Action<object, object>>(Expression.Call(instanceCast, setMethod, valueCast_container), instance_and_value_container).Compile();
						dictionary[p.Name] = setter;
					}
				}
				PropertySetters.TryAdd(type, dictionary);
			}
			return dictionary;
		}// GetPropertySetters()
		#endregion
	}// class ReflectionHelper
}//ns