using System;

namespace SecurityDriven.TinyORM.Extensions
{
	using DbString = Tuple<string, StringType>;

	public static class StringExtensions
	{
		public static DbString NVARCHAR(this string str) => Tuple.Create(str, StringType.NVARCHAR);
		public static DbString NCHAR(this string str) => Tuple.Create(str, StringType.NCHAR);
		public static DbString VARCHAR(this string str) => Tuple.Create(str, StringType.VARCHAR);
		public static DbString CHAR(this string str) => Tuple.Create(str, StringType.CHAR);
	}// class StringExtensions
}//ns