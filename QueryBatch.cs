using System;
using System.Collections.Generic;
using System.Linq;

namespace SecurityDriven.TinyORM
{
	using NameValueTypeTuple = Tuple<string, object, Type>;

	public class QueryBatch
	{
		internal QueryBatch() { }

		int batchSize = DBConstants.DEFAULT_BATCH_SIZE;
		public int BatchSize { get { return batchSize; } }

		internal List<NameValueTypeTuple> queryList = new List<NameValueTypeTuple>();

		public QueryBatch AddQuery<TParamType>(string sql, TParamType param, int batchSize = 0) where TParamType : class
		{
			var query = new NameValueTypeTuple(sql, param, typeof(TParamType));
			queryList.Add(query);
			if (batchSize > 0) this.batchSize = batchSize;
			return this;
		}// AddQuery<TParamType>()

		public QueryBatch AddQuery(string sql, int batchSize = 0)
		{
			this.AddQuery<string>(sql: sql, param: null, batchSize: batchSize);
			return this;
		}// AddQuery() - parameterless

		public QueryBatch AddQuery(QueryInfo queryInfo, int batchSize = 0)
		{
			this.AddQuery(sql: queryInfo.SQL, param: queryInfo.ParameterMap, batchSize: batchSize);
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