using System;

namespace SecurityDriven.TinyORM.Helpers
{
	/// <summary>Type cache.</summary>
	internal static class T<TT>
	{
		/// <summary>typeof(TT).</summary>
		public static readonly Type TypeOf = typeof(TT);
	}
}//ns