using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SecurityDriven.TinyORM.Utils
{
	internal static class TypeConstants
	{
		public static readonly Type ObjectType = typeof(object);
		public static readonly Type NullableType = typeof(Nullable<>);
	}

	#region class ReflectionHelper
	internal static class ReflectionHelper
	{
		public static readonly ConcurrentDictionary<Type, Dictionary<string, Func<object, object>>> PropertyGetters;
		public static readonly ConcurrentDictionary<Type, Dictionary<string, Action<object, object>>> PropertySetters;
		public static readonly ConcurrentDictionary<Type, Dictionary<string, Type>> PropertyTypes;


		static readonly MethodInfo MethodInfo_ConvertAndSet;

		const BindingFlags propertyBindingFlags = (BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

		static ReflectionHelper()
		{
			MethodInfo_ConvertAndSet = typeof(ReflectionHelper).GetMethod("ConvertAndSet", BindingFlags.NonPublic | BindingFlags.Static);
			PropertyTypes = new ConcurrentDictionary<Type, Dictionary<string, Type>>();
			PropertySetters = new ConcurrentDictionary<Type, Dictionary<string, Action<object, object>>>();
			PropertyGetters = new ConcurrentDictionary<Type, Dictionary<string, Func<object, object>>>();
		}// static ctor

		static void ConvertAndSet<T>(object instance, object value, Action<object, object> setter, string propertyName, string typeName)
		{
			var t = typeof(T);
			if
			(
				value == null &&
				t.IsValueType &&
				(t.IsGenericType && !ReferenceEquals(t.GetGenericTypeDefinition(), TypeConstants.NullableType))
			)
			{
				throw new ApplicationException("Cannot convert null into a value type: " + propertyName + " " + t.Name + " " + typeName);
			}

			setter(instance, (T)value);
		}// ConvertAndSet<T>

		public static void GetPropertyTypesAndGetters<T>(T obj, Type objType, out Dictionary<string, Type> propTypeDict, out Dictionary<string, Func<object, object>> propGetterDict)
		{
			propTypeDict = ReflectionHelper.GetPropertyTypes(objType);
			propGetterDict = ReflectionHelper.GetPropertyGetters(objType);
		}// GetPropertyTypesAndGetters<T>

		public static void GetPropertyTypesAndGetters<T>(T obj, out Dictionary<string, Type> propTypeDict, out Dictionary<string, Func<object, object>> propGetterDict)
		{
			GetPropertyTypesAndGetters(obj, typeof(T), out propTypeDict, out propGetterDict);
		}// GetPropertyTypesAndGetters<T>

		public static Dictionary<string, Func<object, object>> GetPropertyGetters<T>(T obj)
		{
			return ReflectionHelper.GetPropertyGetters(typeof(T));
		}// GetPropertyGetters<T>

		public static Dictionary<string, Func<object, object>> GetPropertyGetters(Type type)
		{
			if (!PropertyGetters.TryGetValue(type, out var dictionary))
			{
				IEnumerable<PropertyInfo> source = from p in type.GetProperties(propertyBindingFlags)
												   where p.GetIndexParameters().Length == 0 && !p.ReflectedType.ContainsGenericParameters
												   select p;
				dictionary = new Dictionary<string, Func<object, object>>(source.Count(), Util.FastStringComparer.Instance);
				foreach (PropertyInfo info in source)
				{
					MethodInfo getMethod = info.GetGetMethod(nonPublic: true);
					if (getMethod != null)
					{
						ParameterExpression expression;
						UnaryExpression body = Expression.Convert(Expression.Call(Expression.Convert(expression = Expression.Parameter(TypeConstants.ObjectType, "instance"), type), getMethod), TypeConstants.ObjectType);
						dictionary["@" + info.Name] = Expression.Lambda<Func<object, object>>(body, new ParameterExpression[] { expression }).Compile();
					}
				}
				PropertyGetters.TryAdd(type, dictionary);
			}
			return dictionary;
		}// GetPropertyGetters()

		public static Dictionary<string, Action<object, object>> GetPropertySetters(Type type)
		{
			if (!PropertySetters.TryGetValue(type, out var dictionary))
			{
				IEnumerable<PropertyInfo> source = from p in type.GetProperties(propertyBindingFlags)
												   where p.GetIndexParameters().Length == 0 && !type.IsGenericType
												   select p;
				dictionary = new Dictionary<string, Action<object, object>>(source.Count(), Util.FastStringComparer.Instance);
				foreach (PropertyInfo info in source)
				{
					bool nonPublic = true;
					MethodInfo setMethod = info.GetSetMethod(nonPublic);
					if (setMethod != null)
					{
						var instance = Expression.Parameter(TypeConstants.ObjectType, "instance");
						var value = Expression.Parameter(TypeConstants.ObjectType, "value");

						// value as T is slightly faster than (T)value, so if it's not a value type, use that
						UnaryExpression instanceCast = (type.IsValueType) ?
							Expression.Convert(instance, type) :
							Expression.TypeAs(instance, type);

						UnaryExpression valueCast = Expression.Convert(value, info.PropertyType);

						var setter = Expression.Lambda<Action<object, object>>(Expression.Call(instanceCast, setMethod, valueCast), new[] { instance, value }).Compile();
						dictionary[info.Name] = setter;
					}
				}
				PropertySetters.TryAdd(type, dictionary);
			}
			return dictionary;
		}// GetPropertySetters()

		public static Dictionary<string, Type> GetPropertyTypes<T>(T obj)
		{
			return ReflectionHelper.GetPropertyTypes(typeof(T));
		}// GetPropertyTypes()

		public static Dictionary<string, Type> GetPropertyTypes(Type type)
		{
			if (!PropertyTypes.TryGetValue(type, out var dictionary))
			{
				IEnumerable<PropertyInfo> source = from p in type.GetProperties(propertyBindingFlags)
												   where p.GetIndexParameters().Length == 0
												   select p;
				dictionary = new Dictionary<string, Type>(source.Count(), Util.FastStringComparer.Instance);
				foreach (PropertyInfo info in source)
				{
					dictionary[/*"@" + */info.Name] = info.PropertyType;
				}
				PropertyTypes.TryAdd(type, dictionary);
			}
			return dictionary;
		}// GetPropertyTypes()
	}// class ReflectionHelper
	#endregion
}//ns