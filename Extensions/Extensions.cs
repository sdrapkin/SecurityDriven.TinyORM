using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace SecurityDriven.TinyORM.Extensions
{
	public static class Extensions
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static (TT, Type) WithType<TT>(this TT entity) => (entity, typeof(TT));

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static T[] GetList_itemsArray<T>(this List<T> list) => (T[])new ListUnion { List = list }.ListAccessor.Item1;

		[StructLayout(LayoutKind.Explicit)]
		struct ListUnion
		{
			[FieldOffset(0)]
			public object List;

			[FieldOffset(0)]
			public Tuple<object> ListAccessor;
		}// struct ListUnion
	}// class Extensions
}//ns