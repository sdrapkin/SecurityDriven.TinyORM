using System;

namespace SecurityDriven.TinyORM.Extensions
{
	using DbString = ValueTuple<string, StringType>;

	public static class StringExtensions
	{
		public static DbString NVARCHAR(this string str) => (str, StringType.NVARCHAR);
		public static DbString NCHAR(this string str) => (str, StringType.NCHAR);
		public static DbString VARCHAR(this string str) => (str, StringType.VARCHAR);
		public static DbString CHAR(this string str) => (str, StringType.CHAR);
	}// class StringExtensions
}//ns