using System.Collections.Generic;

namespace SecurityDriven.TinyORM
{
	#region ResultSetSchema
	public sealed class ResultSetSchema
	{
		public int ResultSetId;
		public Dictionary<string, int> FieldMap;
		public string[] FieldNames;

		public ResultSetSchema(int resultSetId, Dictionary<string, int> fieldMap, string[] fieldNames)
		{
			this.ResultSetId = resultSetId;
			this.FieldMap = fieldMap;
			this.FieldNames = fieldNames;
		}//ctor
	}//class ResultSetSchema
	#endregion
}//ns