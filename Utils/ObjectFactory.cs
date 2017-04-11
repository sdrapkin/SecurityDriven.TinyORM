using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM.Utils
{
	#region ObjectFactory
	public static class ObjectFactory
	{
		static readonly ConcurrentDictionary<Type, Func<object, Dictionary<string, object>>> s_type2DictionaryBuilderMap = new ConcurrentDictionary<Type, Func<object, Dictionary<string, object>>>();
		static readonly ConcurrentDictionary<Type, Func<object, Dictionary<string, object>>> s_type2DictionaryBuilderMap_parameterized = new ConcurrentDictionary<Type, Func<object, Dictionary<string, object>>>();

		static readonly ConstructorInfo s_dictionaryCtor = typeof(Dictionary<string, object>).GetConstructor(new[] { typeof(int), typeof(StringComparer) });
		static readonly MethodInfo s_dictionaryAddMethod = typeof(Dictionary<string, object>).GetMethod("Add");
		static readonly ConstantExpression s_nullExpression = Expression.Constant(null);
		static readonly Type s_objectType = typeof(object);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static Dictionary<string, object> ObjectToDictionary<T>(T obj, bool parameterize = false) where T : class => ObjectToDictionary(obj, typeof(T), parameterize);

		public static Dictionary<string, object> ObjectToDictionary(object obj, Type objType, bool parameterize = false)
		{
			if (obj == null) return new Dictionary<string, object>(0, Util.FastStringComparer.Instance);
			if (parameterize) return s_type2DictionaryBuilderMap_parameterized.GetOrAdd(key: objType, valueFactory: MakeToDictionaryParameterizedTrueFunc)(obj);

			return s_type2DictionaryBuilderMap.GetOrAdd(objType, MakeToDictionaryParameterizedFalseFunc)(obj);
		}

		static readonly ConstantExpression fastStringComparerExpression = Expression.Constant(Util.FastStringComparer.Instance);

		static Func<Type, Func<object, Dictionary<string, object>>> MakeToDictionaryParameterizedFalseFunc = type => MakeToDictionaryFunc(type, parameterize: false);

		static Func<Type, Func<object, Dictionary<string, object>>> MakeToDictionaryParameterizedTrueFunc = type => MakeToDictionaryFunc(type, parameterize: true);

		static Func<object, Dictionary<string, object>> MakeToDictionaryFunc(Type type, bool parameterize)
		{
			var param = Expression.Parameter(s_objectType);
			var typed = Expression.Variable(type);

			var elementInitList = GetElementInitsForType(type, typed, parameterize);
			int elementInitListCount = elementInitList.Count();
			if (elementInitListCount == 0) throw new ArgumentException($@"The type ""{type.FullName}"" has no public properties.");

			var newDict = Expression.New(s_dictionaryCtor, Expression.Constant(elementInitListCount), fastStringComparerExpression);
			var listInit = Expression.ListInit(newDict, elementInitList);

			var block = Expression.Block(new[] { typed },
										 Expression.Assign(typed, Expression.Convert(param, type)),
										 listInit);

			return Expression.Lambda<Func<object, Dictionary<string, object>>>(block, param).Compile();
		}

		static IEnumerable<ElementInit> GetElementInitsForType(Type type, Expression param, bool parameterize)
		{
			return from p in type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
				   where p.CanRead
				   select PropertyToElementInit(p, param, parameterize);
		}

		static ElementInit PropertyToElementInit(PropertyInfo propertyInfo, Expression instance, bool parameterize)
		{
			var val = Expression.Convert(Expression.Property(instance, propertyInfo), s_objectType);
			var eq = Expression.ReferenceNotEqual(val, s_nullExpression);
			var condition = Expression.Condition(eq, val, Expression.Convert(Expression.Constant(propertyInfo.PropertyType), s_objectType));

			return Expression.ElementInit(s_dictionaryAddMethod,
				Expression.Constant(parameterize ? "@" + propertyInfo.Name : propertyInfo.Name),
				condition);
		}
	}// class ObjectFactory
	#endregion
}//ns