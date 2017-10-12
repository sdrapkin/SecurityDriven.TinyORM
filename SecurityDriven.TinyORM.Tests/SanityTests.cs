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

	[TestClass]
	public class SanityTests
	{
		static readonly string connString = System.Configuration.ConfigurationManager.ConnectionStrings["Test"].ConnectionString;
		static readonly string marsConnString = connString + ";MultipleActiveResultSets=True";
		static readonly Version tinyormVersion = typeof(DbContext).Assembly.GetName().Version;

		static DbContext db;
		static DbContext marsdb;

		[ClassInitialize]
		public static void Initialize(TestContext testContext)
		{
			db = DbContext.Create(connString);
			marsdb = DbContext.Create(marsConnString);
		}

		[ClassCleanup]
		public static void Cleanup()
		{
		}

		[TestMethod]
		public async Task ConnectionTest()
		{
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
				Assert.IsTrue(query.First().Answer == 125);

				var rows = await db.QueryAsync("select [Answer] = 2 + 3");
				int expectedAnswer = 5;
				Assert.IsTrue(rows[0].Answer == expectedAnswer);
				Assert.IsTrue(rows[0]["Answer"] == expectedAnswer);
				Assert.IsTrue(rows[0][0] == expectedAnswer);

				// single static projection:
				var poco = (rows[0] as RowStore).ToObject<POCO>();
				var poco_via_factory = (rows[0] as RowStore).ToObject(() => new POCO());
				Assert.IsTrue(poco.Answer == expectedAnswer);
				Assert.IsTrue(poco_via_factory.Answer == expectedAnswer);
			}

			{
				// static projection of a list of rows:
				var ids = await db.QueryAsync("select [Answer] = object_id from sys.objects;");
				var pocoArray = ids.ToObjectArray<POCO>();
				var pocoArray_via_factory = ids.ToObjectArray(() => new POCO());
				for (int i = 0; i < pocoArray.Length; ++i)
				{
					Assert.IsTrue(ids[i].Answer > 0);
					Assert.IsTrue(pocoArray[i].Answer > 0);
					Assert.IsTrue(pocoArray_via_factory[i].Answer > 0);
				}
			}
			int low = 10, high = 40;
			{
				var ids1 = await db.QueryAsync("select [Answer] = object_id from sys.objects where object_id between @low and @high;",
					new { @low = low, @high = high });

				Assert.IsTrue(ids1.Count > 0);
				foreach (var row in ids1)
					Assert.IsTrue(row.Answer >= low && row.Answer <= high);
			}
			{
				var ids2 = await db.QueryAsync("select [Answer] = object_id from sys.objects where object_id in (@range)",
					new { @range = Enumerable.Range(low, high - low) });

				Assert.IsTrue(ids2.Count > 0);
				foreach (var row in ids2)
					Assert.IsTrue(row.Answer >= low && row.Answer <= high);
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
					Assert.IsTrue(row.Answer >= low && row.Answer <= high);
			}
			{
				var rows = await db.QueryAsync("select [Answer] = 2 + 3");
				Assert.IsFalse(rows[0].Answer is FieldNotFound); // False
				Assert.IsTrue(rows[0].answer is FieldNotFound); // True
			}
		}

		[TestMethod]
		public async Task SanityTestTransactions()
		{
			{
				var sql = "SELECT [TID]=transaction_id FROM sys.dm_tran_current_transaction; SELECT [TIL]=transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID";
				var (q1_tid, q1_til) = await db.QueryMultipleAsync(sql);
				var (q2_tid, q2_til) = await db.QueryMultipleAsync(sql);

				Assert.IsTrue(q1_til[0].TIL == 2);
				Assert.IsTrue(q2_til[0].TIL == 2);
				Assert.IsTrue(q1_tid[0].TID != q2_tid[0].TID);

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

					Assert.IsTrue(q1_til[0].TIL == 2);
					Assert.IsTrue(q2_til[0].TIL == 2);
					Assert.IsTrue(q1_tid[0].TID == q2_tid[0].TID);

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

					Assert.IsTrue(q1_til[0].TIL == 4);
					Assert.IsTrue(q2_til[0].TIL == 4);
					Assert.IsTrue(q1_tid[0].TID == q2_tid[0].TID);

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

				Assert.IsTrue(q1Task.Result[0].Answer + q2Task.Result[0].Answer == 5);

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

				Assert.IsTrue(result[0].Answer == 123);
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
					if (sqlEx.Message.StartsWith("Execution Timeout Expired.")) return;
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
		}

		[TestMethod]
		public async Task NullTest()
		{
			int expectedAnswer = 123;
			var rows = await db.QueryAsync($"select {expectedAnswer} as Answer union all select null");
			var row0 = rows[0];
			var row1 = rows[1];

			Assert.IsTrue(row0.Answer == expectedAnswer);
			Assert.IsTrue(row1.Answer == null);

			Assert.IsTrue(row0["Answer"] == expectedAnswer);
			Assert.IsTrue(row1["Answer"] == null);

			Assert.IsTrue(row0[0] == expectedAnswer);
			Assert.IsTrue(row1[0] == null);

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
			var array = query.Select(r => r.Value).ToArray();
			CollectionAssert.AreEqual(array, new[] { "abc", "def" });
		}

		[TestMethod]
		public async Task TestBasicStringUsageQueryFirstAsync()
		{
			var query = await db.QueryAsync("select 'abc' as [Value] union all select @txt", new { txt = "def" });
			var str = query.First().Value as string;
			Assert.AreEqual(str, "abc");
		}

		[TestMethod]
		public async Task TestBasicStringUsageQueryFirstOrDefaultAsync()
		{
			var query = await db.QueryAsync("select null as [Value] union all select @txt", new { txt = "def" });
			var str = query.FirstOrDefault() as string;
			Assert.IsNull(str);
		}

		[TestMethod]
		public async Task TestBasicStringUsageQuerySingleAsync()
		{
			var query = await db.QueryAsync("select 'abc' as [Value]");
			var str = query.Single().Value as string;
			Assert.AreEqual(str, "abc");
		}

		[TestMethod]
		public async Task TestBasicStringUsageQuerySingleOrDefaultAsync()
		{
			var query = await db.QueryAsync("select null as [Value]");
			var str = query.Single() as string;
			Assert.IsNull(str);
		}

		[TestMethod]
		public void TestLongOperationWithCancellation()
		{
			CancellationTokenSource cancel = new CancellationTokenSource(TimeSpan.FromSeconds(1));
			var task = db.QueryAsync("waitfor delay '00:00:03';select 1", cancellationToken: cancel.Token).AsTask();
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
			string value = row.Value;
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
			var (q1, q2) = await db.QueryMultipleAsync("select 1; select 2");
			Assert.IsTrue(q1.Single()[0] == 1);
			Assert.IsTrue(q2.Single()[0] == 2);
		}

		[TestMethod]
		public async Task TestMultiAsyncViaFirstOrDefault()
		{
			var (q1, q2, q3, q4, q5) = await db.QueryMultipleAsync("select 1; select 2; select 3; select 4; select 5");
			Assert.IsTrue(q1.FirstOrDefault()[0] == 1);
			Assert.IsTrue(q2.Single()[0] == 2);
			Assert.IsTrue(q3.FirstOrDefault()[0] == 3);
			Assert.IsTrue(q4.Single()[0] == 4);
			Assert.IsTrue(q5.FirstOrDefault()[0] == 5);
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
					int sum = (int)(await db.QueryAsync("select [Sum]=sum(id) + sum(foo) from #literal1")).Single().Sum;
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

					var count = (await db.QueryAsync("select count(1) from #literalin where id in (@ids)", new { ids = new[] { 1, 3, 4 } }))[0][0];
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
			int i = (await db.QueryAsync("select 123"))[0][0];
			Assert.IsTrue(i == 123);

			i = (int)(await db.QueryAsync("select cast(123 as bigint)"))[0][0];
			Assert.IsTrue(i == 123);

			long j = (await db.QueryAsync("select 123"))[0][0];
			Assert.IsTrue(j == 123L);

			j = (await db.QueryAsync("select cast(123 as bigint)"))[0][0];
			Assert.IsTrue(j == 123L);

			int? k = (await db.QueryAsync("select @i", new { i = typeof(long?) }))[0][0];
			Assert.IsTrue(k == null);

			int? m = (await db.QueryAsync("select @i", new { i = typeof(long) }))[0][0];
			Assert.IsTrue(m == null);

			int? n = (await db.QueryAsync("select @i", new { i = default(long?) }))[0][0];
			Assert.IsTrue(n == null);
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

			Assert.IsTrue(result.First().Occupation == "grillmaster");
			Assert.IsTrue(result.First().PersonId == 2);
			Assert.IsTrue(result.First().NumberOfLegs == 1);
			Assert.IsTrue(result.First().AddressName == "bobs burgers");
			Assert.IsTrue(result.First().AddressPersonId == 2);
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

			Assert.IsTrue(query[0].Col1 == null && query[0].Col2 == 1);
			Assert.IsTrue(query[1].Col1 == 2 && query[1].Col2 == null);
			Assert.IsTrue(query[2].Col1 == 4 && query[2].Col2 == 5);
			Assert.IsTrue(query[3].Col1 == null && query[3].Col2 == 7);
			Assert.IsTrue(query[4].Col1 == 8 && query[4].Col2 == null);
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

			Assert.IsTrue(id[0] == 2);
		}

		[TestMethod]
		public async Task QueryAsyncShouldThrowException()
		{
			try
			{
				var data = (await db.QueryAsync("select 1 union all select 2; RAISERROR('after select', 16, 1);"));
				Assert.Fail();
			}
			catch (SqlException ex) when (ex.Message == "after select") { }
		}

		[TestMethod]
		public async Task TestQueryMultipleBuffered()
		{
			var (a, b, c, d) = await db.QueryMultipleAsync("select 1; select 2; select @x; select 4", new { x = 3 });

			Assert.IsTrue(a.Single()[0] == 1);
			Assert.IsTrue(b.Single()[0] == 2);
			Assert.IsTrue(c.Single()[0] == 3);
			Assert.IsTrue(d.Single()[0] == 4);
		}
	}//class
}//ns
