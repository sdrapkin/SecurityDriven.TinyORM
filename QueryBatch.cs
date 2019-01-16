using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM
{
	using Utils;
	using NameValueTypeTuple = ValueTuple<string, Dictionary<string, (object, Type)>>;

	public sealed class QueryBatch
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static QueryBatch Create() => new QueryBatch();
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static QueryBatch Create(IEnumerable<QueryBatch> queryBatchList) => new QueryBatch().Append(queryBatchList);

		internal QueryBatch() { }

		internal List<NameValueTypeTuple> queryList = new List<NameValueTypeTuple>();

		public QueryBatch AddQuery<TParamType>(string sql, TParamType param) where TParamType : class
		{
			Dictionary<string, (object, Type)> paramDictionary;
			if (param == null)
				paramDictionary = new Dictionary<string, (object, Type)>(0);
			else if (param is Dictionary<string, (object, Type)> _paramDictionary)
				paramDictionary = _paramDictionary;
			else
				paramDictionary = ReflectionHelper_Shared.ObjectToDictionary_Parameterized<TParamType>(param);

			var query = new NameValueTypeTuple(sql, paramDictionary);
			queryList.Add(query);
			return this;
		}// AddQuery<TParamType>()

		public QueryBatch AddQuery(string sql)
		{
			this.AddQuery<string>(sql: sql, param: null);
			return this;
		}// AddQuery() - parameterless

		public QueryBatch AddQuery(QueryInfo queryInfo)
		{
			this.AddQuery(sql: queryInfo.SQL, param: queryInfo.ParameterMap);
			return this;
		}// AddQuery() - QueryInfo

		public QueryBatch Append(QueryBatch queryBatch)
		{
			this.queryList.AddRange(queryBatch.queryList);
			return this;
		}// Append()

		public QueryBatch Append(IEnumerable<QueryBatch> queryBatchList)
		{
			foreach (var list in queryBatchList) this.Append(list);
			return this;
		}// Append()
	}//class QueryBatch
}//ns