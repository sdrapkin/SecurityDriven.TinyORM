using System;
using System.Linq;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM.Utils
{
	internal static class Util
	{
		//========== IntToString() begin ==========
		const int MAX_CACHED_INT_TO_STRING_ITEMS = 1000;
		public static System.Globalization.NumberFormatInfo InvariantNFI = System.Globalization.NumberFormatInfo.InvariantInfo;
		static string[] _intToStringCache = Enumerable.Range(0, MAX_CACHED_INT_TO_STRING_ITEMS).Select(i => i.ToString(InvariantNFI)).ToArray();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static string IntToString(ref int value) => value < MAX_CACHED_INT_TO_STRING_ITEMS ? _intToStringCache[value] : value.ToString(InvariantNFI);

		//========== IntToString() end ============

		internal sealed class FastStringComparer : System.Collections.Generic.IEqualityComparer<string>
		{
			FastStringComparer() { }
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public bool Equals(string x, string y) => string.Equals(x, y);
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public int GetHashCode(string obj) => obj.GetHashCode();
			public static FastStringComparer Instance = new FastStringComparer();
		}//class FastStringComparer

		internal sealed class FastTypeComparer : System.Collections.Generic.IEqualityComparer<Type>
		{
			FastTypeComparer() { }
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public bool Equals(Type x, Type y) => x == y;
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public int GetHashCode(Type obj) => RuntimeHelpers.GetHashCode(obj);
			public static FastTypeComparer Instance = new FastTypeComparer();
		}//class FastTypeComparer

		internal static class ZeroLengthArray<T> { public static T[] Value = new T[0]; } // helps avoid unnecessary memory allocation
	}// class Util
}//ns