using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace SecurityDriven.TinyORM.Helpers
{
	using Utils;

	/// <summary>QueryBuilder.</summary>
	public static class QB
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static string AsSqlName<T>(this T obj) => typeof(T).Name.AsSqlName();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static string AsSqlName(this string str) => "[" + str.Replace("]", "]]") + "]";

		const int AVERAGE_PROPERTY_LENGTH = 14 + 2; // assumed average property length, plus 2 characters for "@" and ("," or "=").

		#region Insert<T>()

		public static QueryInfo Insert<T>(T obj, Predicate<string> propFilter = null, string tableName = null) where T : class
		{
			if (tableName == null) tableName = obj.AsSqlName();
			var dict = ObjectFactory.ObjectToDictionary(obj);
			var dictCount = dict.Count;
			var dictNew = new Dictionary<string, object>(dictCount, Util.FastStringComparer.Instance);

			int i = 0;
			string currentKey, paramName;

			var sb = new StringBuilder("INSERT ", AVERAGE_PROPERTY_LENGTH * dictCount * 2 + 25).Append(tableName).Append(" (");
			var sbParams = new StringBuilder(AVERAGE_PROPERTY_LENGTH * dictCount);
			foreach (var kvp in dict)
			{
				currentKey = kvp.Key;
				if (propFilter != null && !propFilter(currentKey))
					continue;

				if (i++ != 0)
				{
					sb.Append(',');
					sbParams.Append(',');
				}
				sb.Append('[').Append(currentKey).Append(']');
				paramName = "@@" + currentKey;
				sbParams.Append(paramName);
				dictNew.Add(paramName, kvp.Value);
			}//foreach

			sb.Append(") VALUES (").Append(sbParams.ToString()).Append(')');
			var result = new QueryInfo { SQL = sb.ToString(), ParameterMap = dictNew };
			return result;
		}// Insert<T>()
		#endregion

		#region Update<T, TParamType>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static QueryInfo Update<T>(T obj, string whereSql = null, Predicate<string> propFilter = null, string tableName = null, Dictionary<string, object> dict = null) where T : class
		{
			return Update<T, string>(obj: obj, whereSql: whereSql, whereParam: null, propFilter: propFilter, tableName: tableName, dict: dict);
		}// Update<T>

		public static QueryInfo Update<T, TParamType>(T obj, string whereSql = null, TParamType whereParam = null, Predicate<string> propFilter = null, string tableName = null, Dictionary<string, object> dict = null) where T : class where TParamType : class
		{
			if (dict == null) dict = ObjectFactory.ObjectToDictionary(obj);
			QueryInfo queryInfo = UpdateRaw<T>(obj, propFilter, tableName, dict);
			var paramDictAlias = queryInfo.ParameterMap;
			var whereParamMap = default(Dictionary<string, object>);

			if (string.IsNullOrEmpty(whereSql))
			{
				if (!dict.TryGetValue("Id", out var id)) throw new ArgumentException(@"""whereSql"" is empty and object does not contain ""Id"" property.");

				whereSql = "Id=@w@Id";
				paramDictAlias.Add("@w@Id", id);
			}
			else if (whereParam != null)
			{
				whereParamMap = whereParam as Dictionary<string, object> ?? ObjectFactory.ObjectToDictionary(whereParam, parameterize: true);
				foreach (var kvp in whereParamMap)
					paramDictAlias.Add(kvp.Key, kvp.Value);
			}
			whereSql = queryInfo.SQL + " WHERE " + whereSql;
			queryInfo = new QueryInfo { SQL = whereSql, ParameterMap = paramDictAlias };
			return queryInfo;
		}// Update<T, TParamType>()
		#endregion

		#region UpdateRaw<T>()
		internal static QueryInfo UpdateRaw<T>(T obj, Predicate<string> propFilter = null, string tableName = null, Dictionary<string, object> dict = null) where T : class
		{
			if (tableName == null) tableName = obj.AsSqlName();
			if (dict == null) dict = ObjectFactory.ObjectToDictionary(obj);
			var dictCount = dict.Count;
			var dictNew = new Dictionary<string, object>(dictCount, Util.FastStringComparer.Instance);

			int i = 0;
			string currentKey, paramName;

			var sb = new StringBuilder("UPDATE ", AVERAGE_PROPERTY_LENGTH * dictCount * 2 + 12).Append(tableName).Append(" SET ");
			foreach (var kvp in dict)
			{
				currentKey = kvp.Key;
				if (propFilter != null && !propFilter(currentKey))
					continue;

				if (i++ != 0)
				{
					sb.Append(',');
				}
				paramName = "@@" + currentKey;
				sb.Append('[').Append(currentKey).Append("]=").Append(paramName);
				dictNew.Add(paramName, kvp.Value);
			}//foreach

			if (i == 0) throw new ArgumentException("propFilter predicate is false for all property names.", "propFilter");
			var result = new QueryInfo { SQL = sb.ToString(), ParameterMap = dictNew };
			return result;
		}// UpdateRaw<T>()
		#endregion

		#region Delete<TParamType>()
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static QueryInfo Delete(string tableName = null, string whereSql = null)
		{
			return Delete<string>(tableName: tableName, whereSql: whereSql);
		}// Delete()

		public static QueryInfo Delete<TParamType>(string tableName = null, string whereSql = null, TParamType whereParam = null) where TParamType : class
		{
			if (string.IsNullOrEmpty(tableName)) throw new ArgumentNullException("tableName");
			if (string.IsNullOrEmpty(whereSql)) throw new ArgumentNullException("whereSql");

			string sql = "DELETE FROM " + tableName + " WHERE " + whereSql;
			var whereParamMap = default(Dictionary<string, object>);

			if (whereParam != null) whereParamMap = whereParam as Dictionary<string, object> ?? ObjectFactory.ObjectToDictionary(whereParam, parameterize: true);
			var result = new QueryInfo { SQL = sql, ParameterMap = whereParamMap };
			return result;
		}// Delete<TParamType>()
		#endregion

		#region Upsert<T>
		public static QueryInfo Upsert<T>(T obj, string tableName = null, Predicate<string> insertPropFilter = null, Predicate<string> updatePropFilter = null, string mergeOnSql = null) where T : class
		{
			if (tableName == null) tableName = obj.AsSqlName();
			if (string.IsNullOrEmpty(mergeOnSql)) mergeOnSql = "S.Id=T.Id";
			var dict = ObjectFactory.ObjectToDictionary(obj);
			var dictCount = dict.Count;
			var dictParams = new Dictionary<string, object>(dictCount, Util.FastStringComparer.Instance);

			string currentKey, currentKeyBracketed, paramName;

			var sbSql = new StringBuilder(";WITH S(");
			var sbParams = new StringBuilder();
			var sbInsertColumns = new StringBuilder();
			var sbInsertValues = new StringBuilder();
			var sbUpdateSql = new StringBuilder();

			int i = 0, insertColumnCount = 0, updateColumnCount = 0;

			foreach (var kvp in dict)
			{
				currentKey = kvp.Key;
				currentKeyBracketed = "[" + currentKey + "]";
				if (i++ != 0)
				{
					sbSql.Append(',');
					sbParams.Append(',');
				}

				sbSql.Append(currentKeyBracketed);
				paramName = "@@" + currentKey;
				sbParams.Append(paramName);
				dictParams.Add(paramName, kvp.Value);

				if (insertPropFilter == null || insertPropFilter(currentKey))
				{
					if (insertColumnCount++ > 0)
					{
						sbInsertColumns.Append(',');
						sbInsertValues.Append(',');
					}
					sbInsertColumns.Append(currentKeyBracketed);
					sbInsertValues.Append(paramName);
				}

				if (updatePropFilter == null || updatePropFilter(currentKey))
				{
					if (updateColumnCount++ > 0)
					{
						sbUpdateSql.Append(',');
					}
					sbUpdateSql.Append(currentKeyBracketed).Append('=').Append(paramName);
				}
			}//foreach

			sbSql
				.Append(") AS (SELECT ")
				.Append(sbParams.ToString())
				.Append(")\nMERGE ")
				.Append(tableName)
				.Append(" WITH (HOLDLOCK) T USING S ON ")
				.Append(mergeOnSql)
				.Append("\nWHEN NOT MATCHED THEN\n\tINSERT (")
				.Append(sbInsertColumns.ToString()).Append(") VALUES (")
				.Append(sbInsertValues);

			if (updateColumnCount > 0)
			{
				sbSql
					.Append(")\nWHEN MATCHED THEN\n\tUPDATE SET ")
					.Append(sbUpdateSql.ToString());
			}
			else
			{
				sbSql.Append(")\n");
			}

			var result = new QueryInfo { SQL = sbSql.ToString(), ParameterMap = dictParams };
			return result;
		}//Upsert()
		#endregion

	}//class QB
}//ns