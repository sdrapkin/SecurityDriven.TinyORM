using System;
using System.Collections.Generic;
using System.Linq;

namespace SecurityDriven.TinyORM
{
	using NameValueTypeTuple = Tuple<string, object, Type>;
	using QueryInfoTuple = Tuple<string, Dictionary<string, object>>;

	public class UnitOfWork
	{
		internal UnitOfWork() { }

		internal List<NameValueTypeTuple> _queryList = new List<NameValueTypeTuple>();

		public UnitOfWork AddQuery<TParamType>(string sql, TParamType param) where TParamType : class
		{
			var query = new NameValueTypeTuple(sql, param, typeof(TParamType));
			_queryList.Add(query);
			return this;
		}// AddQuery<TParamType>()

		public UnitOfWork AddQuery(string sql)
		{
			this.AddQuery<string>(sql: sql, param: null);
			return this;
		}// AddQuery() - parameterless

		public UnitOfWork AddQuery(QueryInfoTuple queryInfo)
		{
			this.AddQuery(sql: queryInfo.Item1, param: queryInfo.Item2);
			return this;
		}// AddQuery() - QueryInfoTuple

		public UnitOfWork Append(UnitOfWork uow)
		{
			this._queryList.AddRange(uow._queryList);
			return this;
		}// Append()

		public UnitOfWork Append(IEnumerable<UnitOfWork> uowList)
		{
			foreach (var list in uowList) this.Append(list);
			return this;
		}// Append()


	}//class UnitOfWork
}//ns