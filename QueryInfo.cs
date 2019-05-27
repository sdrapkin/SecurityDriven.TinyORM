using System;
using System.Collections.Generic;

namespace SecurityDriven.TinyORM
{
	using Utils;

	public sealed class QueryInfo
	{
		public string SQL { get; set; }
		public Dictionary<string, (object, Type)> ParameterMap { get; set; }

		QueryInfo() { }
		QueryInfo(string sql, Dictionary<string, (object, Type)> parameterMap)
		{
			this.SQL = sql;
			this.ParameterMap = parameterMap;
		}

		public static QueryInfo Create(string sql) => Create<string>(sql, null);

		public static QueryInfo Create<TParamType>(string sql, TParamType param) where TParamType : class
		{
			var queryInfo = new QueryInfo() { SQL = sql };
			if (param == null)
			{
				queryInfo.ParameterMap = new Dictionary<string, (object, Type)>(0, Util.FastStringComparer.Instance);
				return queryInfo;
			}

			queryInfo.ParameterMap = ReflectionHelper_Shared.ObjectToDictionary_Parameterized<TParamType>(param);
			return queryInfo;
		}// Create<TParamType>()

		public static QueryInfo Create(string sql, Dictionary<string, (object, Type)> parameterMap) => new QueryInfo(sql, parameterMap);
	}// class QueryInfo
}// ns