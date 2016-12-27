using System;

namespace SecurityDriven.TinyORM.Extensions
{
    using DbString = Tuple<string, StringType?>;

    public static class StringExtensions
	{
		public static DbString NVARCHAR(this string str)
		{
			return Tuple.Create(str, (StringType?)StringType.NVARCHAR);
		}

		public static DbString NCHAR(this string str)
		{
			return Tuple.Create(str, (StringType?)StringType.NCHAR);
		}

		public static DbString VARCHAR(this string str)
		{
			return Tuple.Create(str, (StringType?)StringType.VARCHAR);
		}

		public static DbString CHAR(this string str)
		{
			return Tuple.Create(str, (StringType?)StringType.CHAR);
		}
	}// class StringExtensions
}//ns