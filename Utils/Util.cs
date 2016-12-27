using System.Linq;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM.Utils
{
	internal static class Util
	{
		//========== IntToString() begin ==========
		const int MAX_CACHED_INT_TO_STRING_ITEMS = 1000;
		public static readonly System.Globalization.NumberFormatInfo InvariantNFI = System.Globalization.NumberFormatInfo.InvariantInfo;
		static readonly string[] _intToStringCache = Enumerable.Range(0, MAX_CACHED_INT_TO_STRING_ITEMS).Select(i => i.ToString(InvariantNFI)).ToArray();

		public static string IntToString(this int value)
		{
			return value >= 0 && value < MAX_CACHED_INT_TO_STRING_ITEMS ? _intToStringCache[value] : value.ToString(InvariantNFI);
		}
		//========== IntToString() end ============

		internal sealed class FastStringComparer : System.Collections.Generic.IEqualityComparer<string>
		{
			FastStringComparer() { }
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public bool Equals(string x, string y) { return string.Equals(x, y); }
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public int GetHashCode(string obj) { return obj.GetHashCode(); }
			public static readonly FastStringComparer Instance = new FastStringComparer();
		}//class FastStringComparer

		internal static class ZeroLengthArray<T> { public static readonly T[] Value = new T[0]; } // helps avoid unnecessary memory allocation
	}// class Util
}//ns