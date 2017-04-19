using System.Collections.Generic;

namespace SecurityDriven.TinyORM
{
	using Utils;

	public class QueryInfo
	{
		public string SQL { get; set; }
		public Dictionary<string, object> ParameterMap { get; set; }

		public QueryInfo() { }
		public QueryInfo(string sql, Dictionary<string, object> parameterMap)
		{
			this.SQL = sql;
			this.ParameterMap = parameterMap;
		}

		public static QueryInfo CreateQueryInfo(string sql)
		{
			return CreateQueryInfo<string>(sql, null);
		}// CreateQueryInfo()

		public static QueryInfo CreateQueryInfo<TParamType>(string sql, TParamType param) where TParamType : class
		{
			var queryInfo = new QueryInfo() { SQL = sql };
			if (param == null)
			{
				queryInfo.ParameterMap = new Dictionary<string, object>(0);
				return queryInfo;
			}

			queryInfo.ParameterMap = ReflectionHelper_Shared.ObjectToDictionary<TParamType>(param, ReflectionHelper_Shared.PARAM_PREFIX);
			return queryInfo;
		}// CreateQueryInfo<TParamType>()
	}// class QueryInfo
}// ns