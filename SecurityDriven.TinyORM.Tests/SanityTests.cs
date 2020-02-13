using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace SecurityDriven.TinyORM.Tests
{
	using SecurityDriven.TinyORM;
	using SecurityDriven.TinyORM.Extensions;
	using SecurityDriven.TinyORM.Helpers;

	[TestClass]
	public class SanityTests
	{
		static string connString;
		static string marsConnString = connString + ";MultipleActiveResultSets=True";
		static Version tinyormVersion;

		static DbContext db;
		static DbContext marsdb;

		[ClassInitialize]
		public static void Initialize(TestContext testContext)
		{
#if NETCOREAPP
			var currentDirectory = System.IO.Directory.GetCurrentDirectory();
			connString = new Microsoft.Extensions.Configuration.ConfigurationBuilder().AddXmlFile(currentDirectory + "\\App.config").Build()
				.GetConnectionString("add:Test:connectionString");
#else
			connString = System.Configuration.ConfigurationManager.ConnectionStrings["Test"].ConnectionString;

#endif
			marsConnString = connString + ";MultipleActiveResultSets=True";
			tinyormVersion = typeof(DbContext).Assembly.GetName().Version;

			db = DbContext.Create(connString);
			marsdb = DbContext.Create(marsConnString);
		}

		[ClassCleanup]
		public static void Cleanup()
		{
		}

		[TestMethod]
		public void VersionTest()
		{
			var assembly = typeof(TinyORM.DbContext).Assembly;
			FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
			const string expectedProductVersion = "1.3.2";
			const string expectedFileVersion = "1.3.2.0";

			Assert.IsTrue(fvi.ProductVersion == expectedProductVersion);
			Assert.IsTrue(fvi.FileVersion == expectedFileVersion);

			assembly.GetModules()[0].GetPEKind(out var kind, out var machine);
			Assert.IsTrue(kind == System.Reflection.PortableExecutableKinds.ILOnly);
		}// VersionTest()

		[TestMethod]
		public async Task ConnectionTest()
		{
			var results = await db.QueryAsync("select [Answer] = 1 + 2, [Guid] = NEWID() UNION ALL SELECT 5, 0x");
			Assert.IsTrue(results.Count == 2);

			var firstRow = results[0];
			int expectedAnswer = 3;
			Assert.IsTrue((firstRow as dynamic).Answer == expectedAnswer);
			Assert.IsTrue((int)firstRow["Answer"] == expectedAnswer);
			Assert.IsTrue((int)firstRow[0] == expectedAnswer);
			Assert.IsTrue(firstRow["NonExistentColumn"] is FieldNotFound);
			Assert.IsTrue((firstRow as dynamic).NonExistentColumn is FieldNotFound);

			Assert.IsTrue(firstRow.Schema.FieldMap.Count == 2);

			var secondRow = results[1];
			expectedAnswer = 5;
			Assert.IsTrue((int)secondRow["Answer"] == expectedAnswer);
			((Guid)firstRow["Guid"]).GetHashCode();
			Assert.IsTrue((Guid)secondRow["Guid"] != (Guid)firstRow["Guid"]);
			Assert.IsTrue((Guid)secondRow["Guid"] == Guid.Empty);
			Console.WriteLine(nameof(tinyormVersion) + ": " + tinyormVersion);
		}// ConnectionTest()

		public class POCO
		{
			public int Answer { get; set; }
		}
		[TestMethod]
		public async Task SanityTest()
		{
			{
				var query = await db.QueryAsync("select [Answer] = @a + @b", new { @a = 123, @b = 2 });
				Assert.IsTrue((int)query.First()["Answer"] == 125);

				var rows = await db.QueryAsync("select [Answer] = 2 + 3");
				int expectedAnswer = 5;
				//Assert.IsTrue(rows[0].Answer == expectedAnswer);
				Assert.IsTrue((int)rows[0]["Answer"] == expectedAnswer);
				Assert.IsTrue((int)rows[0][0] == expectedAnswer);

				// single static projection:
				var poco = rows[0].ToObject<POCO>();
				var poco_via_factory = rows[0].ToObject(() => new POCO());
				Assert.IsTrue(poco.Answer == expectedAnswer);
				Assert.IsTrue(poco_via_factory.Answer == expectedAnswer);
			}

			{
				// static projection of a list of rows to RowStore:
				List<RowStore> ids = await db.QueryAsync("select [Answer] = object_id from sys.objects where is_ms_shipped = 1;");
				var pocoArray = ids.ToObjectArray<POCO>();
				var pocoArray_via_factory = ids.ToObjectArray(() => new POCO());
				for (int i = 0; i < pocoArray.Length; ++i)
				{
					Assert.IsTrue((int)ids[i]["Answer"] > 0);
					Assert.IsTrue((int)pocoArray[i].Answer > 0);
					Assert.IsTrue((int)pocoArray_via_factory[i].Answer > 0);
				}
			}
			{
				// static projection of a list of rows to POCO
				List<POCO> pocoList = await db.QueryAsync("select [Answer] = object_id from sys.objects where is_ms_shipped = 1;", default(Func<POCO>));
				foreach (var poco in pocoList)
				{
					Assert.IsTrue(poco.Answer > 0);
				}
			}
			int low = 10, high = 40;
			{
				var ids1 = await db.QueryAsync("select [Answer] = object_id from sys.objects where object_id between @low and @high;",
					new { @low = low, @high = high });

				Assert.IsTrue(ids1.Count > 0);
				foreach (var row in ids1)
					Assert.IsTrue((int)row["Answer"] >= low && (int)row["Answer"] <= high);
			}
			{
				var ids2 = await db.QueryAsync("select [Answer] = object_id from sys.objects where object_id in (@range)",
					new { @range = Enumerable.Range(low, high - low) });

				Assert.IsTrue(ids2.Count > 0);
				foreach (var row in ids2)
					Assert.IsTrue((int)row["Answer"] >= low && (int)row["Answer"] <= high);
			}
			{
				var emptyResult = await db.QueryAsync("select [Answer] = object_id from sys.objects where object_id = @id",
					new { @id = default(int?) }); // or "@id = (int?)null"
				Assert.IsTrue(emptyResult.Count == 0);
			}
			{
				var parameters = new Dictionary<string, (object, Type)>();
				parameters.Add("@low", low.WithType());
				parameters.Add("@high", high.WithType());

				var ids = await db.QueryAsync("select [Answer] = object_id from sys.objects where object_id between @low and @high;", parameters);
				foreach (var row in ids)
					Assert.IsTrue((int)row["Answer"] >= low && (int)row["Answer"] <= high);
			}
			{
				var rows = await db.QueryAsync("select [Answer] = 2 + 3");
				Assert.IsFalse(rows[0]["Answer"] is FieldNotFound); // False
				Assert.IsTrue(rows[0]["answer"] is FieldNotFound); // True
			}
		}

		[TestMethod]
		public async Task SanityTestTransactions()
		{
			{
				var sql = "SELECT [TID]=transaction_id FROM sys.dm_tran_current_transaction; SELECT [TIL]=transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID";
				var (q1_tid, q1_til) = await db.QueryMultipleAsync(sql);
				var (q2_tid, q2_til) = await db.QueryMultipleAsync(sql);

				Assert.IsTrue((short)q1_til[0]["TIL"] == 2);
				Assert.IsTrue((short)q2_til[0]["TIL"] == 2);
				Assert.IsTrue((long)q1_tid[0]["TID"] != (long)q2_tid[0]["TID"]);

				/* "2" is READ COMMITTED
				[transaction_id, 38185] [transaction_isolation_level, 2]
				[transaction_id, 38188] [transaction_isolation_level, 2]
				*/
			}
			{
				var sql = "SELECT [TID]=transaction_id FROM sys.dm_tran_current_transaction; SELECT [TIL]=transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID";
				using (var ts = DbContext.CreateTransactionScope())
				{
					var (q1_tid, q1_til) = await db.QueryMultipleAsync(sql);
					var (q2_tid, q2_til) = await db.QueryMultipleAsync(sql);

					Assert.IsTrue((short)q1_til[0]["TIL"] == 2);
					Assert.IsTrue((short)q2_til[0]["TIL"] == 2);
					Assert.IsTrue((long)q1_tid[0]["TID"] == (long)q2_tid[0]["TID"]);

					ts.Complete();
				}
				/*
				[transaction_id, 41154] [transaction_isolation_level, 2]
				[transaction_id, 41154] [transaction_isolation_level, 2]
				*/
			}
			{
				var sql = "SELECT [TID]=transaction_id FROM sys.dm_tran_current_transaction; SELECT [TIL]=transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID";
				using (var ts = DbContext.CreateTransactionScope(
					TransactionScopeOption.Required,
					new TransactionOptions { IsolationLevel = IsolationLevel.Serializable }))
				{
					var (q1_tid, q1_til) = await db.QueryMultipleAsync(sql);
					var (q2_tid, q2_til) = await db.QueryMultipleAsync(sql);

					Assert.IsTrue((short)q1_til[0]["TIL"] == 4);
					Assert.IsTrue((short)q2_til[0]["TIL"] == 4);
					Assert.IsTrue((long)q1_tid[0]["TID"] == (long)q2_tid[0]["TID"]);

					ts.Complete();
				}
				/* "4" is SERIALIZABLE
				[transaction_id, 42943] [transaction_isolation_level, 4]
				[transaction_id, 42943] [transaction_isolation_level, 4]
				*/
			}
		}

		[TestMethod]
		public async Task SanityTestConcurrentIndependentTransactions()
		{
			// 'outerScope' is some preexisting ambient transaction beyond your control
			using (var outerScope = DbContext.CreateTransactionScope())
			{
				var start = DateTime.UtcNow;
				var q1Task = Task.Run(async () =>
				{
					using (var ts1 = DbContext.CreateTransactionScope(TransactionScopeOption.RequiresNew))
					{
						var result = await db.QueryAsync("WAITFOR DELAY '00:00:02'; SELECT [Answer] = 2;");
						ts1.Complete();
						return result;
					}
				});

				var q2Task = Task.Run(async () =>
				{
					using (var ts2 = DbContext.CreateTransactionScope(TransactionScopeOption.RequiresNew))
					{
						var result = await db.QueryAsync("WAITFOR DELAY '00:00:02'; SELECT [Answer] = 3;");
						ts2.Complete();
						return result;
					}
				});

				await Task.WhenAll(q1Task, q2Task);
				var end = DateTime.UtcNow;
				var time = (end - start);

				Assert.IsTrue((int)q1Task.Result[0]["Answer"] + (int)q2Task.Result[0]["Answer"] == 5);

				var totalSeconds = time.TotalSeconds;
				Console.WriteLine($"{time.TotalSeconds} seconds.");
				Assert.IsTrue(totalSeconds > 2.00 && totalSeconds < 2.1);

				outerScope.Complete();
			}
		}

		[TestMethod]
		public async Task SanityTestQueryTimeouts()
		{
			{
				var start = DateTime.UtcNow;
				var result = await db.QueryAsync("WAITFOR DELAY '00:00:01'; SELECT [Answer] = 123;");
				var duration = DateTime.UtcNow - start;

				Assert.IsTrue((int)result[0]["Answer"] == 123);
				Assert.IsTrue(duration.TotalSeconds > 1.0);
			}
			{
				try
				{
					var start = DateTime.UtcNow;
					var result = await db.QueryAsync("WAITFOR DELAY '00:00:02'; SELECT [Answer] = 123;", commandTimeout: 1);
					var duration = DateTime.UtcNow - start;
				}
				catch (SqlException sqlEx)
				{
					if (sqlEx.Message.IndexOf("Timeout Expired.", StringComparison.OrdinalIgnoreCase) >= 0) return;
				}
				catch (Exception ex)
				{
					ex.GetHashCode();
					throw;
				}
				Assert.Fail("Did not time out as expected");
			}
		}

		[TestMethod]
		public async Task SanityTestBatchedQueries()
		{
			{
				var batch1 = QueryBatch.Create();
				for (int i = 0; i < 50; ++i)
					batch1.AddQuery("select [Answer] = 2;");

				int result1 = await db.CommitQueryBatchAsync(batch1);
				Console.WriteLine(result1);
				Assert.IsTrue(result1 == -1);
				// -1
				// No rows were changed per batch; 1 batch only (default batch size is 50).

				var batch2 = QueryBatch.Create();
				for (int i = 0; i < 65; ++i)
					batch2.AddQuery("select [Answer] = 2;");

				int result2 = await db.CommitQueryBatchAsync(batch2);
				Console.WriteLine(result2);
				Assert.IsTrue(result2 == -1);
				// -1
				// No rows were changed per batch; 1 batch only (short 2nd batch of 15 queries merged into the 1st batch)
				// short batch is 50/3 = 16 queries or less
			}
			{
				var batch3 = QueryBatch.Create();
				for (int i = 0; i < 70; ++i)
					batch3.AddQuery("select [Answer] = 2;");

				int result3 = await db.CommitQueryBatchAsync(batch3);
				Console.WriteLine(result3);
				Assert.IsTrue(result3 == -2);
				// -2
				// No rows were changed per batch; 2 batches:
				// 1st batch of 50 queries and 2nd batch of 20 queries
				// last batch is larger than short batch - triggers an additional db call
			}
			{
				var batch1 = QueryBatch.Create();
				var batch2 = QueryBatch.Create();
				for (int i = 0; i < 40; ++i)
				{
					batch1.AddQuery("select [Answer] = 1;");
					batch2.AddQuery("select [Answer] = 2;");
				}
				batch2.Append(batch1); // adding batch1 queries to batch2
				int result = await db.CommitQueryBatchAsync(QueryBatch.Create(new[] { batch1, batch2 }));
				Console.WriteLine(result);
				Assert.IsTrue(result == -3);
				// -3
				// No rows were changed per batch; 3 batches:
				// 1st batch of 50 queries; 2nd batch of 50 queries; 3rd batch of 20 queries
				// last batch is larger than short batch - triggers an additional db call
			}
			{
				var batch = QueryBatch.Create();
				for (int i = 0; i < 43; ++i)
					batch.AddQuery("select [Answer] = 1;");

				int result = await db.CommitQueryBatchAsync(queryBatch: batch, batchSize: 10);
				Console.WriteLine(result);
				Assert.IsTrue(result == -4);
				// -4
				// 3 batches with 10 queries and last batch with 13 queries
			}
			{
				var dbTemp = DbContext.Create(db.ConnectionString);
				var batch = QueryBatch.Create();
				for (int i = 0; i < 43; ++i)
					batch.AddQuery("select [Answer] = 1;");

				dbTemp.BatchSize = 10;
				int result = await dbTemp.CommitQueryBatchAsync(batch);
				Console.WriteLine(result);
				Assert.IsTrue(result == -4);
				// -4
				// 3 batches with 10 queries and last batch with 13 queries
			}
			{
				var queryBatch = QueryBatch.Create();
				var queryInfo = QueryInfo.Create("declare @foo table(id int not null); insert @foo values (@id), (@id), (@id);", new { id = 37 });
				queryBatch.AddQuery(queryInfo);
				int result = await db.CommitQueryBatchAsync(queryBatch);
				Console.WriteLine(result);
				Assert.IsTrue(result == 3);
			}
		}

		[TestMethod]
		public async Task NullTest()
		{
			int expectedAnswer = 123;
			var rows = await db.QueryAsync($"select {expectedAnswer} as Answer union all select null");
			var row0 = rows[0];
			var row1 = rows[1];

			Assert.IsTrue((int?)row0["Answer"] == expectedAnswer);
			Assert.IsTrue((int?)row1["Answer"] == null);

			Assert.IsTrue((int?)row0["Answer"] == expectedAnswer);
			Assert.IsTrue((int?)row1["Answer"] == null);

			Assert.IsTrue((int?)row0[0] == expectedAnswer);
			Assert.IsTrue((int?)row1[0] == null);

			var objects = rows.ToObjectArray<NullTestObject>();
			Assert.IsTrue(objects[0].Answer == expectedAnswer);
		}// NullTest()

		// https://github.com/StackExchange/Dapper/blob/master/Dapper.Tests/Tests.Async.cs

		#region POCOs
		class NullTestObject
		{
			public int? Answer { get; set; }
		}
		class BasicType
		{
			public string Value { get; set; }
		}

		class Product
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public Category Category { get; set; }
		}

		class Category
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public string Description { get; set; }
		}

		class Person
		{
			public int PersonId { get; set; }
			public string Name { get; set; }
			public string Occupation { get; private set; }
			public int NumberOfLegs = 2;
			public Address Address { get; set; }
		}

		class Address
		{
			public int AddressId { get; set; }
			public string Name { get; set; }
			public int PersonId { get; set; }
		}
		#endregion

		[TestMethod]
		public async Task TestBasicStringUsageAsync()
		{
			var query = await db.QueryAsync("select 'abc' as [Value] union all select @txt", new { txt = "def" });
			var array = query.Select(r => r["Value"] as string).ToArray();
			CollectionAssert.AreEqual(array, new[] { "abc", "def" });
		}

		[TestMethod]
		public async Task TestBasicStringUsageQueryFirstAsync()
		{
			var query = await db.QueryAsync("select 'abc' as [Value] union all select @txt", new { txt = "def" });
			var str = query.First()["Value"] as string;
			Assert.AreEqual(str, "abc");
		}

		[TestMethod]
		public async Task TestBasicStringUsageQueryFirstOrDefaultAsync()
		{
			var query = await db.QueryAsync("select null as [Value] union all select @txt", new { txt = "def" });
			var str = query.FirstOrDefault()["Value"] as string;
			Assert.IsNull(str);
		}

		[TestMethod]
		public async Task TestBasicStringUsageQuerySingleAsync()
		{
			var query = await db.QueryAsync("select 'abc' as [Value]");
			var str = query.Single()["Value"] as string;
			Assert.AreEqual(str, "abc");
		}

		[TestMethod]
		public async Task TestBasicStringUsageQuerySingleOrDefaultAsync()
		{
			var query = await db.QueryAsync("select null as [Value]");
			var str = query.Single()["Value"] as string;
			Assert.IsNull(str);
		}

		[TestMethod]
		public void TestLongOperationWithCancellation()
		{
			CancellationTokenSource cancel = new CancellationTokenSource(TimeSpan.FromSeconds(1));
			var task = db.QueryAsync("waitfor delay '00:00:03';select 1", cancellationToken: cancel.Token);
			try
			{
				if (!task.Wait(TimeSpan.FromSeconds(2)))
				{
					throw new TimeoutException(); // should have cancelled
				}
			}
			catch (AggregateException agg)
			{
				Assert.IsTrue(agg.InnerException is SqlException);
				Assert.IsTrue((agg.InnerException as SqlException).Errors[1].ToString() == "System.Data.SqlClient.SqlError: Operation cancelled by user.");
			}
		}

		[TestMethod]
		public async Task TestQueryDynamicAsync()
		{
			var row = (await db.QueryAsync("select 'abc' as [Value]")).Single();
			string value = row["Value"] as string;
			Assert.AreEqual(value, "abc");
		}

		[TestMethod]
		public async Task TestClassWithStringUsageAsync()
		{
			var query = await db.QueryAsync("select 'abc' as [Value], 123 as [IntValue] union all select @txt, 456", new { txt = "def" });
			var arr = query.ToObjectArray<BasicType>();
			CollectionAssert.AreEqual(arr.Select(x => x.Value).ToArray(), new[] { "abc", "def" });
		}

		[TestMethod]
		public async Task TestExecuteAsync()
		{
			var queryBatch = QueryBatch.Create();
			queryBatch.AddQuery("declare @foo table(id int not null); insert @foo values (@id), (@id), (@id);", new { id = 37 });
			var val = await db.CommitQueryBatchAsync(queryBatch);
			Assert.AreEqual(val, 3);
		}

		[TestMethod]
		public async Task TestQueryMultipleWithDestructuring()
		{
			const string sql = @"select 1 as Id, 'abc' as Name; select 2 as Id, 'def' as Name";
			var (products, categories) = await db.QueryMultipleAsync(sql);

			Product product = products.First().ToObject<Product>();
			Category category = categories.First().ToObject<Category>();
			// assertions
			Assert.IsTrue(product.Id == 1);
			Assert.IsTrue(product.Name == "abc");
			Assert.IsTrue(category.Id == 2);
			Assert.IsTrue(category.Name == "def");
		}

		[TestMethod]
		public async Task TestMultiAsync()
		{
			var (q1, q2, q3) = await db.QueryMultipleAsync("select 1; select 2; select 3");
			Assert.IsTrue((int)q1.Single()[0] == 1);
			Assert.IsTrue(q1.Single().Schema.ResultSetId == 0);

			Assert.IsTrue((int)q2.Single()[0] == 2);
			Assert.IsTrue(q2.Single().Schema.ResultSetId == 1);

			Assert.IsTrue((int)q3.Single()[0] == 3);
			Assert.IsTrue(q3.Single().Schema.ResultSetId == 2);
		}

		[TestMethod]
		public async Task TestMultiAsyncViaFirstOrDefault()
		{
			var (q1, q2, q3, q4, q5) = await db.QueryMultipleAsync("select 1; select 2; select 3; select 4; select 5");
			Assert.IsTrue((int)q1.FirstOrDefault()[0] == 1);
			Assert.IsTrue((int)q2.Single()[0] == 2);
			Assert.IsTrue((int)q3.FirstOrDefault()[0] == 3);
			Assert.IsTrue((int)q4.Single()[0] == 4);
			Assert.IsTrue((int)q5.FirstOrDefault()[0] == 5);
		}

		[TestMethod]
		public async Task TestSequentialReaderAsync()
		{
			var dt = new System.Data.DataTable();
			var result = await db.SequentialReaderAsync("select 3 as [three], 4 as [four]", async (reader, ct) =>
			{
				dt.Load(reader);
				return await Task.FromResult<bool>(true);
			});
			Assert.IsTrue(result);
			Assert.IsTrue(dt.Columns.Count == 2);
			Assert.IsTrue(dt.Columns[0].ColumnName == "three");
			Assert.IsTrue(dt.Columns[1].ColumnName == "four");
			Assert.IsTrue(dt.Rows.Count == 1);
			Assert.IsTrue((int)dt.Rows[0][0] == 3);
			Assert.IsTrue((int)dt.Rows[0][1] == 4);
		}

		[TestMethod]
		public async Task TestQueryAsyncAsTextOnly()
		{
			// outside of TransactionScope, since otherwise XACT_ABORT=ON and transaction is aborted
			try { await db.QueryAsync("drop table literal1"); } catch { }

			using (var ts = DbContext.CreateTransactionScope())
			{
				bool textFlag = true;
				try
				{
					await db.QueryAsync("create table #literal1 (id int not null, foo int not null)", sqlTextOnly: textFlag);
					const string sqlInsert = "insert #literal1 (id,foo) values (@id, @foo)";
					await db.QueryAsync(sqlInsert, new { id = 123, foo = 456 });
					foreach (var p in new[] { new { id = 1, foo = 2 }, new { id = 3, foo = 4 } })
						await db.QueryAsync(sqlInsert, p);

					var count = (int)(await db.QueryAsync("select [Count]=count(1) from #literal1 where id=@foo", new { foo = 123 })).Single().Count;
					Assert.IsTrue(count == 1);
					int sum = (int)(await db.QueryAsync("select [Sum]=sum(id) + sum(foo) from #literal1")).Single()["Sum"];
					Assert.IsTrue(sum == 123 + 456 + 1 + 2 + 3 + 4);
				}
				catch (Exception ex)
				{
					ex.GetHashCode();
					throw;
				}
				ts.Complete();
			}//ts
		}

		[TestMethod]
		public async Task TestXACT_ABORT()
		{
			var sanity = await db.QueryAsync("SELECT 2 + 2;");
			Assert.IsTrue((int)sanity[0][0] == (2 + 2));

			await Assert.ThrowsExceptionAsync<TransactionAbortedException>(async () =>
			{
				using (var ts = DbContext.CreateTransactionScope())
				{
					// do something that should abort the transaction
					try { await db.QueryAsync("something that is definitely not valid T-SQL"); }
					catch (Exception ex)
					{
						Assert.IsTrue(ex is SqlException);
						Console.WriteLine(ex.Message);
					}

					// now do something normal, which should throw TransactionAbortedException
					await db.QueryAsync("select 1 + 2");
					ts.Complete();
				}
			});
		}

		[TestMethod]
		public async Task LiteralInAsync()
		{
			try
			{
				using (var ts = DbContext.CreateTransactionScope())
				{
					await db.QueryAsync("create table #literalin(id int not null);", sqlTextOnly: true);
					await db.QueryAsync("insert #literalin (id) values (@id1), (@id2), (@id3)", new { id1 = 1, id2 = 2, id3 = 3 });

					int count = (int)(await db.QueryAsync("select count(1) from #literalin where id in (@ids)", new { ids = new[] { 1, 3, 4 } }))[0][0];
					Assert.IsTrue(count == 2);
				}
			}
			catch (Exception ex)
			{
				ex.GetHashCode();
				throw;
			}
		}

		[TestMethod]
		public async Task RunSequentialVersusParallelAsync()
		{
			var ids = Enumerable.Range(1, 2090).Select(id => new { id }).ToArray();

			var sqlBuilder = new StringBuilder();
			string sql;
			var @params = new Dictionary<string, (object, Type)>();
			for (int i = 0; i < ids.Length; ++i)
			{
				sqlBuilder.Append($"select @id${i};");
				@params.Add($"@id${i}", (i, i.GetType()));
			}
			sql = sqlBuilder.ToString();

			var sw = new Stopwatch();

			for (int i = 0; i < 3; ++i)
			{
				sw.Restart();
				await db.QueryAsync(sql, @params);
				sw.Stop();
				Console.WriteLine("{0, -10} {1} ms", "Regular:", sw.ElapsedMilliseconds);

				sw.Restart();
				await marsdb.QueryAsync(sql, @params);
				sw.Stop();
				Console.WriteLine("{0, -10} {1} ms", "MARS:", sw.ElapsedMilliseconds);
			}
		}

		[TestMethod]
		public async Task TestExecuteScalarAsync()
		{
			{
				int i = (int)(await db.QueryAsync("select 123"))[0][0];
				Assert.IsTrue(i == 123);
			}

			{
				int i = (int)(long)(await db.QueryAsync("select cast(123 as bigint)"))[0][0];
				Assert.IsTrue(i == 123);
			}
			{
				long j = (long)(int)(await db.QueryAsync("select 123"))[0][0];
				Assert.IsTrue(j == 123L);
			}
			{
				long j = (long)(await db.QueryAsync("select cast(123 as bigint)"))[0][0];
				Assert.IsTrue(j == 123L);
			}
			{
				// should work since "long?" to "int?" is convertible for null
				long? k = (int?)(await db.QueryAsync("select @i", new { i = typeof(long?) }))[0][0];
				Assert.IsTrue(k == null);
			}
			{
				await Assert.ThrowsExceptionAsync<InvalidCastException>(async () =>
				{
					int? k = (int?)(await db.QueryAsync("select @i", new { i = (long?)123 }))[0][0];
				}); // should throw, because "long?" to "int?" is not convertible for non-null
			}
			{
				// should work since "int?" to "long?" is convertible for non-null
				long? k = (int?)(await db.QueryAsync("select @i", new { i = (int?)123 }))[0][0];
				Assert.IsTrue(k == 123L);
			}
			{
				// should work because the query returns NULL, which is convertible to any nullable struct
				int? m = (int?)(await db.QueryAsync("select @i", new { i = typeof(long) }))[0][0];
				Assert.IsTrue(m == null);
			}
			{
				await Assert.ThrowsExceptionAsync<InvalidCastException>(async () =>
				{
					int? m = (int?)(await db.QueryAsync("select @i", new { i = (long)123 }))[0][0];
				}); // should throw, because "long" to "int?" is not convertible for non-null
			}
			{
				int? n = (int?)(await db.QueryAsync("select @i", new { i = default(long?) }))[0][0];
				Assert.IsTrue(n == null);
			}
			{
				long? p = (long?)(await db.QueryAsync("select @i", new { i = default(long?) }))[0][0];
				Assert.IsTrue(p == null);
			}
		}

		[TestMethod]
		public async Task TestSupportForDynamicParametersOutputExpressionsAsync()
		{
			var bob = new Person { Name = "bob", PersonId = 1, Address = new Address { PersonId = 2 } };

			var p = new
			{
				bob.PersonId,
				bob.Occupation,
				bob.NumberOfLegs,
				AddressName = bob.Address.Name,
				AddressPersonId = bob.Address.PersonId
			};
			var result = await db.QueryAsync(@"
				SET @Occupation = 'grillmaster' 
				SET @PersonId = @PersonId + 1 
				SET @NumberOfLegs = @NumberOfLegs - 1
				SET @AddressName = 'bobs burgers'
				SET @AddressPersonId = @PersonId
				SELECT
					@Occupation [Occupation],
					@PersonId [PersonId],
					@NumberOfLegs [NumberOfLegs],
					@AddressName [AddressName],
					@AddressPersonId [AddressPersonId]", p);

			Assert.IsTrue((string)result.First()["Occupation"] == "grillmaster");
			Assert.IsTrue((int)result.First()["PersonId"] == 2);
			Assert.IsTrue((int)result.First()["NumberOfLegs"] == 1);
			Assert.IsTrue((string)result.First()["AddressName"] == "bobs burgers");
			Assert.IsTrue((int)result.First()["AddressPersonId"] == 2);
		}

		[TestMethod]
		public async Task TestDynamicParameters()
		{
			var parameters = new Dictionary<string, (object, Type)>();
			for (int i = 0; i < 10; ++i)
			{
				var pval = i % 3 == 0 ? default(int?) : i;
				parameters.Add("p" + i, pval.WithType());
			}

			var sql =
				@"SELECT * FROM
				(
					VALUES
						(@p0, @p1),
						(@p2, @p3),
						(@p4 ,@p5),
						(@p6, @p7),
						(@p8 ,@p9)
				) t(Col1, Col2);";

			var query = await db.QueryAsync(sql, parameters);

			Assert.IsTrue((int?)query[0]["Col1"] == null && (int?)query[0]["Col2"] == 1);
			Assert.IsTrue((int?)query[1]["Col1"] == 2 && (int?)query[1]["Col2"] == null);
			Assert.IsTrue((int?)query[2]["Col1"] == 4 && (int?)query[2]["Col2"] == 5);
			Assert.IsTrue((int?)query[3]["Col1"] == null && (int?)query[3]["Col2"] == 7);
			Assert.IsTrue((int?)query[4]["Col1"] == 8 && (int?)query[4]["Col2"] == null);
		}

		[TestMethod]
		public async Task TestSubsequentQueriesSuccessAsync()
		{
			var data0 = (await db.QueryAsync("select 1 as [Id] where 1 = 0")).ToObjectArray<AsyncFoo0>();
			Assert.IsTrue(data0.Length == 0);

			var data1 = (await db.QueryAsync(QueryInfo.Create("select 1 as [Id] where 1 = 0"))).ToObjectArray<AsyncFoo1>();
			Assert.IsTrue(data1.Length == 0);

			var data2 = (await db.QueryAsync(QueryInfo.Create("select 1 as [Id] where 1 = 0"))).ToObjectArray<AsyncFoo2>();
			Assert.IsTrue(data2.Length == 0);

			data0 = (await db.QueryAsync("select 1 as [Id] where 1 = 0")).ToObjectArray<AsyncFoo0>();
			Assert.IsTrue(data0.Length == 0);

			data1 = (await db.QueryAsync(QueryInfo.Create("select 1 as [Id] where 1 = 0"))).ToObjectArray<AsyncFoo1>();
			Assert.IsTrue(data1.Length == 0);

			data2 = (await db.QueryAsync(QueryInfo.Create("select 1 as [Id] where 1 = 0"))).ToObjectArray<AsyncFoo2>();
			Assert.IsTrue(data2.Length == 0);
		}

		class AsyncFoo0 { public int Id { get; set; } }
		class AsyncFoo1 { public int Id { get; set; } }
		class AsyncFoo2 { public int Id { get; set; } }

		[TestMethod]
		public async Task TestSchemaChangedViaFirstOrDefaultAsync()
		{
			using (var ts = DbContext.CreateTransactionScope())
			{
				await db.QueryAsync("create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, 'Alf')", sqlTextOnly: true);
				try
				{
					var d = (await db.QueryAsync("select * from #dog", sqlTextOnly: true)).ToObjectArray<Dog>()[0];
					Assert.IsTrue(d.Name == "Alf");
					Assert.IsTrue(d.Age == 1);

					await db.QueryAsync("alter table #dog drop column Name", sqlTextOnly: true);
					d = (await db.QueryAsync("select * from #dog")).ToObjectArray<Dog>()[0];
					Assert.IsTrue(d.Name == null);
					Assert.IsTrue(d.Age == 1);
				}
				catch (Exception ex)
				{
					ex.GetHashCode();
					throw;
				}
				finally
				{
					await db.QueryAsync("drop table #dog", sqlTextOnly: true);
				}
				ts.Complete();
			}
		}

		class Dog
		{
			public int Age { get; set; }
			public string Name { get; set; }
		}

		[TestMethod]
		public async Task TestAtEscaping()
		{
			var id = (await db.QueryAsync(@"
                declare @@Name int
                select @@Name = @Id+1
                select @@Name
                ", new { Id = 1, Name = default(string) })).Single();

			Assert.IsTrue((int)id[0] == 2);
		}

		[TestMethod]
		public async Task QueryAsyncShouldThrowException()
		{
			try
			{
				var data = (await db.QueryMultipleAsync("select 1 union all select 2; RAISERROR('after select', 16, 1);").ConfigureAwait(false));
				Assert.Fail();
			}
			catch (SqlException ex) when (ex.Message == "after select") { }
		}

		[TestMethod]
		public async Task TestQueryMultipleBuffered()
		{
			var (a, b, c, d) = await db.QueryMultipleAsync("select 1; select 2; select @x; select 4", new { x = 3 });

			Assert.IsTrue((int)a.Single()[0] == 1);
			Assert.IsTrue((int)b.Single()[0] == 2);
			Assert.IsTrue((int)c.Single()[0] == 3);
			Assert.IsTrue((int)d.Single()[0] == 4);
		}

		[TestMethod]
		public async Task TestEmptyParameterObject()
		{
			var result = await db.QueryAsync("select 1;", new { });
			Assert.IsTrue((int)result.Single()[0] == 1);
		}

		[TestMethod]
		public async Task TestTransactionScopes_and_ParallelQueries()
		{
			var sw = Stopwatch.StartNew();

			using (var outer_ts = DbContext.CreateTransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = IsolationLevel.RepeatableRead }))
			{
				await ParallelQueries().ConfigureAwait(false);
				outer_ts.Complete();
			}//outer_ts
			sw.Stop();
			Assert.IsTrue(sw.Elapsed < TimeSpan.FromSeconds(2.0), $"Took {sw.Elapsed}"); // all parallel queries should take 1 second each.

			async Task ParallelQueries()
			{
				var tasks = new List<Task<IReadOnlyList<RowStore>>>();

				for (int i = 1; i <= 5; ++i)
					tasks.Add(
						((Func<Task<IReadOnlyList<RowStore>>>)(async () =>
						{
							var j = i;
							using (var ts = DbContext.CreateTransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions { IsolationLevel = (j % 2 == 0 ? IsolationLevel.ReadCommitted : IsolationLevel.Serializable) }))
							{
								var result = await db.QueryAsync($"WAITFOR DELAY '00:00:01'; select [Answer]={j}, [TID] = CURRENT_TRANSACTION_ID(), transaction_isolation_level from sys.dm_exec_sessions where session_id = @@spid;").ConfigureAwait(false);
								ts.Complete();
								return result;
							}
						}))()
					);

				await Task.WhenAll(tasks).ConfigureAwait(false);
				var sum = tasks.Sum(t => (int)t.Result.First()["Answer"]);
				Assert.IsTrue(sum == 15);

				for (int i = 1; i <= 5; ++i)
					Assert.IsTrue((short)tasks[i - 1].Result.First()["transaction_isolation_level"] == (i % 2 == 0 ? 2 : 4)); // 2=ReadCommitted; 4=Serializable
			}// ParallelQueries(s)
		}// TestTransactionScopes()

		[TestMethod]
		public async Task TestTVP()
		{
			try
			{
				await db.QueryAsync("BEGIN TRY CREATE TYPE __GuidTable AS TABLE (Id UNIQUEIDENTIFIER PRIMARY KEY CLUSTERED);END TRY BEGIN CATCH END CATCH");

				const int COUNT = 3000;
				var data = Enumerable.Range(0, COUNT).Select(i => Guid.NewGuid()).ToArray();
				var tvp = data.AsTVP("__GuidTable");
				var result = await db.QueryAsync("select * from @tvp", new { tvp });
				Guid[] guids = result.Cast<dynamic>().Select(row => (Guid)row.Id).ToArray();

				Assert.IsTrue(guids.Length == COUNT);
			}
			finally
			{
				await db.QueryAsync("DROP TYPE __GuidTable;");

			}
		}// TestTVP()

		[TestMethod]
		public async Task Test_Version_PropName()
		{
			var p = new
			{
				Id = new Guid("cf9fad7a-9775-28b9-7693-11e6ea3b1484"),
				Name = "John",
				BirthDate = new DateTime(1975, 03, 17),
				Version = Environment.TickCount
			};

			using (var ts = DbContext.CreateTransactionScope())
			{
				await db.QueryAsync("create table #Person (Id uniqueidentifier primary key, Name nvarchar(50), BirthDate datetime2, Version timestamp);", sqlTextOnly: true);
				var query = Helpers.QB.Upsert(p, tableName: "#Person",
				excludedInsertProperties: n => (n == "Version"),
				includedUpdateProperties: n => (n != "Version" && n != "Id")
				);

				var batch = QueryBatch.Create();
				batch.AddQuery(query);

				await db.CommitQueryBatchAsync(batch);

				dynamic row = (await db.QueryAsync("select * from #Person;")).Single();
				Assert.IsTrue(row.Id == p.Id && row.Name == p.Name && row.BirthDate == p.BirthDate);
				await db.QueryAsync("drop table #Person;");
			}
		}// Test_Version_PropName()

		[TestMethod]
		public void Test_QB_Insert_Update()
		{
			var person = new
			{
				Id = new Guid("cf9fad7a-9775-28b9-7693-11e6ea3b1484"),
				Name = "John",
				BirthDate = new DateTime(1975, 03, 17),
				Version = Environment.TickCount
			};

			string tableName = "CustomTableName";

			string n1 = nameof(person.Id);
			string n2 = nameof(person.Name);
			string n3 = nameof(person.BirthDate);
			string n4 = nameof(person.Version);

			string p1 = $"@#{n1}";
			string p2 = $"@#{n2}";
			string p3 = $"@#{n3}";
			string p4 = $"@#{n4}";

			(object, Type) p = default;

			{   // Insert - all properties
				var query = QB.Insert(person, tableName: tableName.AsSqlName());
				string expectedSQL = $"INSERT [{tableName}] ([{n1}],[{n2}],[{n3}],[{n4}]) VALUES ({p1},{p2},{p3},{p4})";

				Assert.IsTrue(query.SQL == expectedSQL);
				Assert.IsTrue(query.ParameterMap.Count == 4);

				p = query.ParameterMap[p1];
				Assert.IsTrue((Guid)p.Item1 == person.Id);
				Assert.IsTrue(p.Item2 == typeof(Guid));

				p = query.ParameterMap[p2];
				Assert.IsTrue((string)p.Item1 == person.Name);
				Assert.IsTrue(p.Item2 == typeof(string));

				p = query.ParameterMap[p3];
				Assert.IsTrue((DateTime)p.Item1 == person.BirthDate);
				Assert.IsTrue(p.Item2 == typeof(DateTime));

				p = query.ParameterMap[p4];
				Assert.IsTrue((int)p.Item1 == person.Version);
				Assert.IsTrue(p.Item2 == typeof(int));
			}
			{   // Insert - excluded properties
				var query = QB.Insert(person, excludedProperties: n => n == nameof(person.Version), tableName: tableName.AsSqlName());
				string expectedSQL = $"INSERT [{tableName}] ([{n1}],[{n2}],[{n3}]) VALUES ({p1},{p2},{p3})";

				Assert.IsTrue(query.SQL == expectedSQL);
				Assert.IsTrue(query.ParameterMap.Count == 3);

				p = query.ParameterMap[p1];
				Assert.IsTrue((Guid)p.Item1 == person.Id);
				Assert.IsTrue(p.Item2 == typeof(Guid));

				p = query.ParameterMap[p2];
				Assert.IsTrue((string)p.Item1 == person.Name);
				Assert.IsTrue(p.Item2 == typeof(string));

				p = query.ParameterMap[p3];
				Assert.IsTrue((DateTime)p.Item1 == person.BirthDate);
				Assert.IsTrue(p.Item2 == typeof(DateTime));
			}

			string p5 = $"@w@{n1}";
			{   // Update - all properties
				var query = QB.Update(person, tableName: tableName.AsSqlName());
				string expectedSQL = $"UPDATE [{tableName}] SET [{n1}]={p1},[{n2}]={p2},[{n3}]={p3},[{n4}]={p4} WHERE [{n1}]={p5}";

				Assert.IsTrue(query.SQL == expectedSQL);
				Assert.IsTrue(query.ParameterMap.Count == 5);

				p = query.ParameterMap[p1];
				Assert.IsTrue((Guid)p.Item1 == person.Id);
				Assert.IsTrue(p.Item2 == typeof(Guid));

				p = query.ParameterMap[p2];
				Assert.IsTrue((string)p.Item1 == person.Name);
				Assert.IsTrue(p.Item2 == typeof(string));

				p = query.ParameterMap[p3];
				Assert.IsTrue((DateTime)p.Item1 == person.BirthDate);
				Assert.IsTrue(p.Item2 == typeof(DateTime));

				p = query.ParameterMap[p4];
				Assert.IsTrue((int)p.Item1 == person.Version);
				Assert.IsTrue(p.Item2 == typeof(int));

				p = query.ParameterMap[p5];
				Assert.IsTrue((Guid)p.Item1 == person.Id);
				Assert.IsTrue(p.Item2 == typeof(Guid));
			}
			{   // Update - included properties - one property
				var query = QB.Update(person, includedProperties: n => n == nameof(person.Name), tableName: tableName.AsSqlName());
				string expectedSQL = $"UPDATE [{tableName}] SET [{n2}]={p2} WHERE [{n1}]={p5}";

				Assert.IsTrue(query.SQL == expectedSQL);
				Assert.IsTrue(query.ParameterMap.Count == 2);

				p = query.ParameterMap[p2];
				Assert.IsTrue((string)p.Item1 == person.Name);
				Assert.IsTrue(p.Item2 == typeof(string));

				p = query.ParameterMap[p5];
				Assert.IsTrue((Guid)p.Item1 == person.Id);
				Assert.IsTrue(p.Item2 == typeof(Guid));
			}
			{   // Update - included properties - all except two
				var query = QB.Update(person, includedProperties: n => n != nameof(person.Id) && n != nameof(person.Version), tableName: tableName.AsSqlName());
				string expectedSQL = $"UPDATE [{tableName}] SET [{n2}]={p2},[{n3}]={p3} WHERE [{n1}]={p5}";

				Assert.IsTrue(query.SQL == expectedSQL);
				Assert.IsTrue(query.ParameterMap.Count == 3);

				p = query.ParameterMap[p2];
				Assert.IsTrue((string)p.Item1 == person.Name);
				Assert.IsTrue(p.Item2 == typeof(string));

				p = query.ParameterMap[p3];
				Assert.IsTrue((DateTime)p.Item1 == person.BirthDate);
				Assert.IsTrue(p.Item2 == typeof(DateTime));

				p = query.ParameterMap[p5];
				Assert.IsTrue((Guid)p.Item1 == person.Id);
				Assert.IsTrue(p.Item2 == typeof(Guid));
			}
		}// Test_QB_Insert_Update()
	}//class SanityTests
}//ns
