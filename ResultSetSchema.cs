using System.Collections.Generic;

namespace SecurityDriven.TinyORM
{
	#region ResultSetSchema
	public class ResultSetSchema
	{
		public readonly int ResultSetId;
		public readonly Dictionary<string, int> FieldMap;
		public readonly string[] FieldNames;

		public ResultSetSchema(int resultSetId, Dictionary<string, int> fieldMap, string[] fieldNames)
		{
			this.ResultSetId = resultSetId;
			this.FieldMap = fieldMap;
			this.FieldNames = fieldNames;
		}//ctor
	}//class ResultSetSchema
	#endregion
}//ns