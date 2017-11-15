using System;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM.Extensions
{
	using SecurityDriven.TinyORM.Helpers;

	public static class Extensions
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static (TT, Type) WithType<TT>(this TT entity) => (entity, typeof(TT));
	}// class Extensions
}//ns