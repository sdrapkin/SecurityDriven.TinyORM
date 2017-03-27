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
		public static T[] ToObjectArray<T>(this IReadOnlyList<dynamic> listOfDynamic) where T : class, new() => ToObjectArray<T>(listOfDynamic, New<T>.Instance);

		/// <summary>Converts a List of RowStore-objects into an array of T-objects on a best-effort-match basis. Parallelized. Does not throw on any mismatches.</summary>
		public static T[] ToObjectArray<T>(this IReadOnlyList<dynamic> listOfDynamic, Func<T> objectFactory) where T : class
		{
			var listOfRowStore = listOfDynamic as IReadOnlyList<RowStore>;
			int newListCount = listOfRowStore.Count;
			T[] newList = new T[newListCount];
			if (newListCount == 0) return newList;

			RowStore firstElement = listOfRowStore[0];
			int fieldCount = firstElement.RowValues.Length;

			var settersArray = new Action<object, object>[fieldCount];
			var setters = ReflectionHelper.GetPropertySetters(typeof(T));
			foreach (var setter in setters)
			{
				if (firstElement.Schema.FieldMap.TryGetValue(setter.Key, out var index))
					settersArray[index] = setter.Value;
			}

			Parallel.For(0, newListCount, s_ParallelOptions, i =>
			{
				T objT = objectFactory();
				newList[i] = objT;
				object[] rowValues = (listOfRowStore[i]).RowValues;
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

		/// <summary>Converts a List of dynamic objects into an array of T-objects using a provided object mapper. Parallelized.</summary>
		public static T[] ToMappedObjectArray<T>(this IReadOnlyList<dynamic> listOfDynamic, Func<dynamic, T> objectMapper)
		{
			var listOfObject = listOfDynamic as IReadOnlyList<object>;
			int newListCount = listOfObject.Count;
			T[] newList = new T[newListCount];
			if (newListCount == 0) return newList;

			Parallel.For(0, newListCount, s_ParallelOptions, i =>
			{
				newList[i] = objectMapper(listOfObject[i]);
			});
			return newList;
		}//ToMappedObjectArray<T>

		#region Deconstructors
		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0) { v0 = v[0]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1) { v0 = v[0]; v1 = v[1]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1, out T v2) { v0 = v[0]; v1 = v[1]; v2 = v[2]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1, out T v2, out T v3) { v0 = v[0]; v1 = v[1]; v2 = v[2]; v3 = v[3]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1, out T v2, out T v3, out T v4) { v0 = v[0]; v1 = v[1]; v2 = v[2]; v3 = v[3]; v4 = v[4]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1, out T v2, out T v3, out T v4, out T v5) { v0 = v[0]; v1 = v[1]; v2 = v[2]; v3 = v[3]; v4 = v[4]; v5 = v[5]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1, out T v2, out T v3, out T v4, out T v5, out T v6) { v0 = v[0]; v1 = v[1]; v2 = v[2]; v3 = v[3]; v4 = v[4]; v5 = v[5]; v6 = v[6]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1, out T v2, out T v3, out T v4, out T v5, out T v6, out T v7) { v0 = v[0]; v1 = v[1]; v2 = v[2]; v3 = v[3]; v4 = v[4]; v5 = v[5]; v6 = v[6]; v7 = v[7]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1, out T v2, out T v3, out T v4, out T v5, out T v6, out T v7, out T v8) { v0 = v[0]; v1 = v[1]; v2 = v[2]; v3 = v[3]; v4 = v[4]; v5 = v[5]; v6 = v[6]; v7 = v[7]; v8 = v[8]; }

		///<summary>Deconstructor.</summary>
		public static void Deconstruct<T>(this IReadOnlyList<T> v, out T v0, out T v1, out T v2, out T v3, out T v4, out T v5, out T v6, out T v7, out T v8, out T v9) { v0 = v[0]; v1 = v[1]; v2 = v[2]; v3 = v[3]; v4 = v[4]; v5 = v[5]; v6 = v[6]; v7 = v[7]; v8 = v[8]; v9 = v[9]; }
		#endregion
	}// class DynamicListExtensions
}//ns