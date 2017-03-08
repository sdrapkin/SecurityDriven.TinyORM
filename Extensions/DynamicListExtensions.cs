using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SecurityDriven.TinyORM.Extensions
{
	using Utils;

	public static class DynamicListExtensions
	{
		static readonly ParallelOptions s_ParallelOptions = new ParallelOptions { TaskScheduler = TaskScheduler.Default };

		/// <summary>Converts a List of RowStore-objects into an array of T-objects on a best-effort-match basis. Parallelized. Does not throw on any mismatches.</summary>
		public static T[] ToObjectArray<T>(this IReadOnlyList<dynamic> listOfDynamic, Func<T> objectFactory) where T : class
		{
			var setters = ReflectionHelper.GetPropertySetters(typeof(T));
			int newListCount = listOfDynamic.Count;
			T[] newList = new T[newListCount];
			if (newListCount == 0) return newList;

			RowStore firstElement = (RowStore)listOfDynamic[0];
			int fieldCount = firstElement.RowValues.Length;

			var settersArray = new Action<object, object>[fieldCount];
			foreach (var setter in setters)
			{
				if (firstElement.Schema.FieldMap.TryGetValue(setter.Key, out var index))
					settersArray[index] = setter.Value;
			}

			Parallel.For(0, newListCount, s_ParallelOptions, i =>
			{
				T objT = objectFactory();
				newList[i] = objT;
				object[] rowValues = ((RowStore)listOfDynamic[i]).RowValues;
				Action<object, object> setter;
				for (int j = 0; j < rowValues.Length; ++j)
				{
					setter = settersArray[j];
					if (setter != null)
					{
						object val = rowValues[j];
						setter(objT, val == DBNull.Value ? null : val);
					}
				}
			});
			return newList;
		}// ToObjectArray<T>()
	}// class DynamicListExtensions
}//ns