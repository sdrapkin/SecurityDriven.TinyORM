using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace SecurityDriven.TinyORM.Utils
{
	static class ReflectionHelper<T>
	{
		static readonly Type ObjectType = typeof(object);
		const BindingFlags propertyBindingFlags = (BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

		public static readonly Dictionary<string, Action<T, object>> Setters = GetPropertySetters();
		#region Property Setters
		static Dictionary<string, Action<T, object>> GetPropertySetters()
		{
			var type = typeof(T);

			var source = type.ContainsGenericParameters ? Util.ZeroLengthArray<PropertyInfo>.Value : type.GetProperties(propertyBindingFlags);
			var dictionary = new Dictionary<string, Action<T, object>>(source.Length, Util.FastStringComparer.Instance);

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
					var setter = Expression.Lambda<Action<T, object>>(Expression.Call(instanceCast, setMethod, valueCast_container), instance_and_value_container).Compile();
					dictionary[p.Name] = setter;
				}
			}//foreach PropertyInfo

			return dictionary;
		}// GetPropertySetters()
		#endregion
	}// class ReflectionHelper<T>
}//ns