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
			var queryInfo = new QueryInfo();
			queryInfo.SQL = sql;
			if (param == null)
			{
				queryInfo.ParameterMap = new Dictionary<string, object>(0);
				return queryInfo;
			}

			queryInfo.ParameterMap = ObjectFactory.ObjectToDictionary(obj: param, objType: typeof(TParamType), parameterize: true);
			return queryInfo;
		}// CreateQueryInfo<TParamType>()
	}// class QueryInfo
}// ns