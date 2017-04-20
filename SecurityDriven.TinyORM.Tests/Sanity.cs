using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;

namespace SecurityDriven.TinyORM.Tests
{
	using SecurityDriven.TinyORM;

	[TestClass]
	public class Sanity
	{
		static readonly string connString = System.Configuration.ConfigurationManager.ConnectionStrings["Test"].ConnectionString;
		static readonly Version tinyormVersion = typeof(DbContext).Assembly.GetName().Version;

		[ClassInitialize]
		public static void Initialize(TestContext testContext)
		{
		}

		[ClassCleanup]
		public static void Cleanup()
		{
		}

		[TestMethod]
		public async Task ConnectionTest()
		{
			var db = DbContext.CreateDbContext(connString);
			var results = await db.QueryAsync("select [Answer] = 1 + 2, [Guid] = NEWID() UNION ALL SELECT 5, 0x");
			Assert.IsTrue(results.Count == 2);

			var firstRow = results[0];
			int expectedAnswer = 3;
			Assert.IsTrue(firstRow.Answer == expectedAnswer);
			Assert.IsTrue(firstRow["Answer"] == expectedAnswer);
			Assert.IsTrue(firstRow[0] == expectedAnswer);
			Assert.IsTrue(firstRow.NonExistentColumn is FieldNotFound);
			Assert.IsTrue((firstRow as RowStore).Schema.FieldMap.Count == 2);

			var secondRow = results[1];
			expectedAnswer = 5;
			Assert.IsTrue(secondRow.Answer == expectedAnswer);
			((Guid)firstRow.Guid).GetHashCode();
			Assert.IsTrue((Guid)secondRow.Guid != (Guid)firstRow.Guid);
			Assert.IsTrue((Guid)secondRow.Guid == Guid.Empty);
			Console.WriteLine(tinyormVersion);
		}
	}//class
}//ns