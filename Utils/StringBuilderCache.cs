using System;
using System.Runtime.CompilerServices;
using System.Text;

namespace SecurityDriven.TinyORM.Utils
{
	// Based on https://referencesource.microsoft.com/#mscorlib/system/text/stringbuildercache.cs
	internal static class StringBuilderCache
	{
		[ThreadStatic]
		static StringBuilder _cachedInstance;
		const int MAX_SIZE = 1024;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static StringBuilder Acquire(int capacity = 16)
		{
			if (capacity <= MAX_SIZE)
			{
				StringBuilder cachedInstance = _cachedInstance;
				if ((cachedInstance != null) && (capacity <= cachedInstance.Capacity))
				{
					_cachedInstance = null;
					cachedInstance.Length = 0;
					return cachedInstance;
				}
			}
			return new StringBuilder(capacity);
		}// Acquire()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static string GetStringAndRelease(StringBuilder sb)
		{
			string @string = sb.ToString();
			Release(sb);
			return @string;
		}// GetStringAndRelease()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void Release(StringBuilder sb)
		{
			if (sb.Capacity <= MAX_SIZE)
			{
				_cachedInstance = sb;
			}
		}// Release()
	}//class StringBuilderCache
}//ns