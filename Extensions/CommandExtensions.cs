using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM.Extensions
{
	using Utils;

	using DbString = ValueTuple<string, StringType>;

	internal static class CommandExtensions
	{
		#region GenerateParameter()
		static object GenerateParameter(string parameterName, object parameterValue, Type parameterType)
		{
			var p = new SqlParameter() { ParameterName = parameterName };

			if (parameterValue == null)
			{
				var dbType = typeMap[parameterType];
				p.DbType = dbType;
				if (dbType == DbType.Binary) p.Size = -1;
				p.Value = DBNull.Value;
				return p;
			}

			if (parameterValue is ValueType dataValue)
			{
				// if data is DateTime, switch to higher precision of DateTime2
				if (parameterType == s_DateTimeType) p.DbType = DbType.DateTime2;

				p.Value = parameterValue;
				return p;
			}
			else
			{
				var stringType = StringType.NVARCHAR;

				// check if data is DbString
				if (parameterValue is DbString dbString)
				{
					parameterValue = dbString.Item1;
					stringType = dbString.Item2;
					p.DbType = (DbType)stringType;
				}

				switch (parameterValue)
				{
					// check if data is regular string
					case string dataString:
						int lenThreshold = (stringType == StringType.NVARCHAR || stringType == StringType.NCHAR) ? MAX_UNICODE_STRING_LENGTH : MAX_ANSI_STRING_LENGTH;
						p.Size = dataString.Length > lenThreshold ? -1 : lenThreshold;
						p.Value = parameterValue;
						return p;

					// check if data is NULL
					case Type type:
						var dbType = typeMap[type];
						p.DbType = dbType;
						if (dbType == DbType.Binary) p.Size = -1;
						p.Value = DBNull.Value;
						return p;

					// check if data is TVP
					case DataTable dataTable:
						p.SqlDbType = SqlDbType.Structured;
						p.TypeName = dataTable.TableName;
						p.Value = dataTable;
						return p;
				}//switch on parameterValue type
			}// data is not a struct

			p.Value = parameterValue;
			return p;
		}// GenerateParameter()

		static readonly Type s_DateTimeType = typeof(DateTime);
		#endregion

		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		static void ProcessParameter(ref SqlCommand command, ref SqlParameterCollection sqlParameterCollection, ref string name, ref object value, ref Type type)
		{
			if (value is IEnumerable propValueEnumerable && !(value is string s) && !(value is byte[] b))
			{
				int count = 0;
				Type enumerableType = null;

				foreach (var item in propValueEnumerable)
				{
					++count;
					if (count == 1) enumerableType = item.GetType();
					var sqlParameter = GenerateParameter(parameterName: name + Util.IntToString(ref count), parameterValue: item, parameterType: enumerableType);
					sqlParameterCollection.Add(sqlParameter);
				}
				command.CommandText = command.CommandText.Replace(name, count == 0 ? "SELECT TOP 0 0" : GetParamString(count, name));
			}
			else
			{
				var sqlParameter = GenerateParameter(parameterName: name, parameterValue: value, parameterType: type);
				sqlParameterCollection.Add(sqlParameter);
			}
		}// ProcessParameter()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		static void SetParametersFromContainerObject<Param>(this SqlCommand command, Param param) where Param : class
		{
			var sqlParameterCollection = command.Parameters;
			var paramGettersDictionary = ReflectionHelper_ParameterizedGetter<Param>.Getters;

			var paramGettersDictionaryEnumerator = paramGettersDictionary.GetEnumerator();
			while (paramGettersDictionaryEnumerator.MoveNext())
			{
				var kvp = paramGettersDictionaryEnumerator.Current;
				var propName = kvp.Key;

				(object propValue, Type propType) = kvp.Value(param);
				ProcessParameter(ref command, ref sqlParameterCollection, ref propName, ref propValue, ref propType);
			}// while over property dictionary enumerator
		}// SetParametersFromContainerObject<Param>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static void SetParametersFromDictionary(this SqlCommand command, Dictionary<string, (object, Type)> paramDictionary)
		{
			var sqlParameterCollection = command.Parameters;

			//if (paramDictionary.Count > 0)
			{
				var paramDictionaryEnumerator = paramDictionary.GetEnumerator();
				while (paramDictionaryEnumerator.MoveNext())
				{
					var kvp = paramDictionaryEnumerator.Current;
					var propName = kvp.Key;

					(object propValue, Type propType) = kvp.Value;
					ProcessParameter(ref command, ref sqlParameterCollection, ref propName, ref propValue, ref propType);
				}// while over property dictionary enumerator
			}// if dictionary count > 0
		}// SetParametersFromDictionary()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static void SetupParameters<TParamType>(this SqlCommand command, TParamType param) where TParamType : class
		{
			{
				if (!(param is Dictionary<string, (object, Type)> dictParam))
					command.SetParametersFromContainerObject<TParamType>(param);
				else
					command.SetParametersFromDictionary(dictParam);
			}
		}// SetupParameters<TParamType>()

		#region internal constants
		internal const string CTX_PARAMETER_NAME = "@@ctx";
		internal const string CT_PARAMETER_NAME = "@@ct";
		internal const string CI_PARAMETER_NAME = "@@ci";

		internal const string CMD_HEADER_START = "set nocount,xact_abort on;";
		internal const string CMD_HEADER_START_QUERYBATCH = "set xact_abort on;";

		// 112 is [MAX_CONTEXT_INFO_LENGTH minus size_of_ctx_guid, 128-16
		internal const string CMD_HEADER_REST = "set " + CI_PARAMETER_NAME + "=" + CTX_PARAMETER_NAME + "+cast(right(" + CT_PARAMETER_NAME + ",112)as binary(112));set context_info " + CI_PARAMETER_NAME + ";\n";

		internal const string CMD_HEADER = CMD_HEADER_START + CMD_HEADER_REST;
		internal const string CMD_HEADER_QUERYBATCH = CMD_HEADER_START_QUERYBATCH + CMD_HEADER_REST;
		internal const string CMD_FOOTER = ";\nset context_info 0";

		internal const int MAX_UNICODE_STRING_LENGTH = 4000;
		internal const int MAX_ANSI_STRING_LENGTH = MAX_UNICODE_STRING_LENGTH * 2;
		internal const int MAX_CONTEXT_INFO_LENGTH = 128;
		#endregion

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static void SetupMetaParameters(this SqlCommand command, byte[] callerIdentity, string callerMemberName, string callerFilePath, int callerLineNumber)
		{
			var parametersCollection = command.Parameters;
			// CONTEXT parameter
			{
				object callerIdentityContextParameter = new SqlParameter(CTX_PARAMETER_NAME, SqlDbType.Binary, 16) { Value = callerIdentity };
				parametersCollection.Add(callerIdentityContextParameter);
			}
			// CODETRACE parameter
			{
				object codetraceParameter = new SqlParameter(CT_PARAMETER_NAME, SqlDbType.VarChar, MAX_ANSI_STRING_LENGTH)
				{
					Value = MakeCodeTraceString(callerMemberName, callerFilePath, callerLineNumber)
				};
				parametersCollection.Add(codetraceParameter);
			}
			// CONTEXT_INFO parameter
			{
				object ciParameter = new SqlParameter(CI_PARAMETER_NAME, SqlDbType.VarBinary, MAX_CONTEXT_INFO_LENGTH)
				{
					Value = Util.ZeroLengthArray<byte>.Value
				};
				parametersCollection.Add(ciParameter);
			}
		}// SetupMetaParameters()

		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static void Setup<TParamType>(
			this SqlCommand command,
			string sql,
			TParamType param,
			byte[] callerIdentity,
			int? commandTimeout = null,
			bool sqlTextOnly = false,
			string callerMemberName = null,
			string callerFilePath = null,
			int callerLineNumber = 0
		) where TParamType : class
		{
			command.CommandText = sql;
			if (!sqlTextOnly)
			{
				if (param != null)
					SetupParameters<TParamType>(command, param);

				SetupMetaParameters(command, callerIdentity, callerMemberName, callerFilePath, callerLineNumber);

				command.CommandText = string.Concat(CMD_HEADER, command.CommandText, CMD_FOOTER);
			}

			if (commandTimeout.HasValue)
				command.CommandTimeout = commandTimeout.GetValueOrDefault();
		}// Setup<TparamType>()

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static string MakeCodeTraceString(string callerMemberName, string callerFilePath, int callerLineNumber)
		{
			return string.Concat(callerFilePath, Util.IntToString(ref callerLineNumber), callerMemberName);
		}

		static string GetParamString(int count, string propName)
		{
			var sb = Utils.StringBuilderCache.Acquire((propName.Length + 1) * (count + 1)).Append(propName).Append('1');
			for (int i = 2; i <= count; ++i)
			{
				sb.Append(',').Append(propName).Append(Util.IntToString(ref i));
			}
			return Utils.StringBuilderCache.GetStringAndRelease(sb);
		}// GetParamString()

		static readonly Dictionary<Type, DbType> typeMap = new Dictionary<Type, DbType>
		{
			{ typeof(byte),  DbType.Byte },
			{ typeof(sbyte), DbType.SByte },
			{ typeof(short), DbType.Int16 },
			{ typeof(ushort), DbType.UInt16},
			{ typeof(int), DbType.Int32 },
			{ typeof(uint), DbType.UInt32 },
			{ typeof(long), DbType.Int64 },
			{ typeof(ulong), DbType.UInt64 },
			{ typeof(float), DbType.Single },
			{ typeof(double), DbType.Double },
			{ typeof(decimal), DbType.Decimal },
			{ typeof(bool), DbType.Boolean },
			{ typeof(string), DbType.String },
			{ typeof(char), DbType.StringFixedLength },
			{ typeof(Guid), DbType.Guid },
			{ typeof(DateTime), DbType.DateTime2 },
			{ typeof(DateTimeOffset), DbType.DateTimeOffset },
			{ typeof(TimeSpan), DbType.Time },
			{ typeof(byte[]), DbType.Binary },
			{ typeof(byte?), DbType.Byte },
			{ typeof(sbyte?), DbType.SByte },
			{ typeof(short?), DbType.Int16 },
			{ typeof(ushort?), DbType.UInt16 },
			{ typeof(int?), DbType.Int32 },
			{ typeof(uint?), DbType.UInt32 },
			{ typeof(long?), DbType.Int64 },
			{ typeof(ulong?), DbType.UInt64 },
			{ typeof(float?), DbType.Single },
			{ typeof(double?), DbType.Double },
			{ typeof(decimal?), DbType.Decimal },
			{ typeof(bool?), DbType.Boolean },
			{ typeof(char?), DbType.StringFixedLength },
			{ typeof(Guid?), DbType.Guid },
			{ typeof(DateTime?), DbType.DateTime2 },
			{ typeof(DateTimeOffset?), DbType.DateTimeOffset },
			{ typeof(TimeSpan?), DbType.Time },
			{ typeof(Object), DbType.Object }
		};//typeMap
	}//class CommandExtensions
}//ns