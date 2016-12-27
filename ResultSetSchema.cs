using System.Collections.Generic;

namespace SecurityDriven.TinyORM
{
	#region ResultSetSchema
	public class ResultSetSchema
	{
		public readonly int ResultSetId;
		public readonly Dictionary<string, int> FieldMap;

		public ResultSetSchema(int resultSetId, Dictionary<string, int> fieldMap)
		{
			this.ResultSetId = resultSetId;
			this.FieldMap = fieldMap;
		}
	}//class ResultSetSchema
	#endregion
}//ns