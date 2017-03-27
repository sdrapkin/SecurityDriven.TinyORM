using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM
{
	using NameValueTypeTuple = Tuple<string, object, Type>;

	public class QueryBatch
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static QueryBatch CreateQueryBatch() => new QueryBatch();
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static QueryBatch CreateQueryBatch(IEnumerable<QueryBatch> queryBatchList) => new QueryBatch().Append(queryBatchList);

		internal QueryBatch() { }

		internal List<NameValueTypeTuple> queryList = new List<NameValueTypeTuple>();

		public QueryBatch AddQuery<TParamType>(string sql, TParamType param) where TParamType : class
		{
			var query = new NameValueTypeTuple(sql, param, typeof(TParamType));
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