using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace SecurityDriven.TinyORM.Extensions
{
	using Utils;

	public static class RowStoreListExtensions
	{
		/// <summary>Converts a List of RowStore into an array of T-objects on a best-effort-match basis. Parallelized. Does not throw on any mismatches.</summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static T[] ToObjectArray<T>(this List<RowStore> listOfRowStore) where T : class, new() => ToObjectArray<T>(listOfRowStore, New<T>.Instance);

		/// <summary>Converts a List of RowStore-objects into an array of T-objects on a best-effort-match basis. Parallelized. Does not throw on any mismatches.</summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static T[] ToObjectArray<T>(this List<RowStore> listOfRowStore, Func<T> objectFactory) where T : class
		{
			unchecked
			{
				var listOfRowStoreAlias = listOfRowStore;
				int newArrayLength = listOfRowStoreAlias.Count;
				T[] newArray = new T[newArrayLength];
				if (newArrayLength == 0) return newArray;

				var objectFactoryAlias = objectFactory;
				RowStore[] arrayOfRowStore = listOfRowStoreAlias.GetList_itemsArray();
				RowStore firstElement = arrayOfRowStore[0];

				var settersArray = new Action<T, object>[firstElement.RowValues.Length];

				var fieldNames = firstElement.Schema.FieldNames;
				var settersMap = ReflectionHelper_Setter<T>.Setters;
				for (int i = 0; i < fieldNames.Length; ++i)
				{
					if (settersMap.TryGetValue(fieldNames[i], out var setter))
						settersArray[i] = setter;
				}

				Parallel.For(0, newArrayLength, i =>
				{
					T objT = objectFactoryAlias();
					newArray[i] = objT;
					object[] rowValues = arrayOfRowStore[i].RowValues;

					for (i = 0; i < rowValues.Length; ++i)
					{
						settersArray[i]?.Invoke(objT, rowValues[i]);
					}//for
				});
				return newArray;
			}//unchecked
		}// ToObjectArray<T>()

		/// <summary>Converts a List of RowStore into an array of T-objects using a provided object mapper. Parallelized.</summary>
		public static T[] ToMappedObjectArray<T>(this List<RowStore> listOfRowStore, Func<RowStore, T> objectMapper) where T : class
		{
			var listOfRowStoreAlias = listOfRowStore;
			int newArrayLength = listOfRowStoreAlias.Count;
			T[] newArray = new T[newArrayLength];
			if (newArrayLength == 0) return newArray;
			var objectMapperAlias = objectMapper;

			RowStore[] arrayOfRowStore = listOfRowStoreAlias.GetList_itemsArray();

			Parallel.For(0, newArrayLength, i => newArray[i] = objectMapperAlias(arrayOfRowStore[i]));
			return newArray;
		}//ToMappedObjectArray<T>

		/// <summary>Converts any Array or List of T into a single-column named TVP.</summary>
		public static DataTable AsTVP<T>(this IReadOnlyList<T> list, string tvpName)
		{
			var dataTable = new DataTable(tvpName);
			dataTable.Columns.Add();

			var rows = dataTable.Rows;
			var paramsContainer = new object[1];
			ref var val = ref paramsContainer[0];
			var count = list.Count;

			for (int i = 0; i < count; ++i)
			{
				val = list[i];
				rows.Add(paramsContainer);
			}
			return dataTable;
		}//AsTVP<T>

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
	}// class RowStoreListExtensions
}//ns