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
