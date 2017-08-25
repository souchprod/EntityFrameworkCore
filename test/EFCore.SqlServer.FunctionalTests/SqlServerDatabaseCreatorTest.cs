// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.TestUtilities.Xunit;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

// ReSharper disable InconsistentNaming
namespace Microsoft.EntityFrameworkCore
{
    // Tests are split into classes to enable parallel execution
    // Some combinations are skipped to reduce run time
    public class SqlServerDatabaseCreatorExistsTest
    {
        [ConditionalTheory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public Task Returns_false_when_database_does_not_exist(bool async, bool ambientTransaction)
        {
            return Returns_false_when_database_does_not_exist_test(async, ambientTransaction, file: false);
        }

        [ConditionalTheory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [SqlServerCondition(SqlServerCondition.SupportsAttach)]
        public Task Returns_false_when_database_with_filename_does_not_exist(bool async, bool ambientTransaction)
        {
            return Returns_false_when_database_does_not_exist_test(async, ambientTransaction, file: true);
        }

        private static async Task Returns_false_when_database_does_not_exist_test(bool async, bool ambientTransaction, bool file)
        {
            using (var testDatabase = SqlServerTestStore.Create("NonExisting", file))
            {
                using (var context = new SqlServerDatabaseCreatorTest.BloggingContext(testDatabase))
                {
                    var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(context);

                    using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                    {
                        Assert.False(async ? await creator.ExistsAsync() : creator.Exists());
                    }

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
                }
            }
        }

        [ConditionalTheory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        public Task Returns_true_when_database_exists(bool async, bool ambientTransaction)
        {
            return Returns_true_when_database_exists_test(async, ambientTransaction, file: false);
        }

        [ConditionalTheory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        [SqlServerCondition(SqlServerCondition.SupportsAttach)]
        public Task Returns_true_when_database_with_filename_exists(bool async, bool ambientTransaction)
        {
            return Returns_true_when_database_exists_test(async, ambientTransaction, file: true);
        }

        private static async Task Returns_true_when_database_exists_test(bool async, bool ambientTransaction, bool file)
        {
            using (var testDatabase = file
                ? SqlServerTestStore.CreateInitialized("ExistingBloggingFile", useFileName: true)
                : SqlServerTestStore.GetOrCreateInitialized("ExistingBlogging"))
            {
                using (var context = new SqlServerDatabaseCreatorTest.BloggingContext(testDatabase))
                {
                    var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(context);
                    using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                    {
                        Assert.True(async ? await creator.ExistsAsync() : creator.Exists());
                    }

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
                }
            }
        }
    }

    public class SqlServerDatabaseCreatorEnsureDeletedTest
    {
        [ConditionalTheory]
        [InlineData(true, true, true)]
        [InlineData(true, false, false)]
        [InlineData(false, true, false)]
        [InlineData(false, false, true)]
        public Task Deletes_database(bool async, bool open, bool ambientTransaction)
        {
            return Delete_database_test(async, open, ambientTransaction, file: false);
        }

        [ConditionalTheory]
        [InlineData(true, true, false)]
        [InlineData(true, false, true)]
        [InlineData(false, true, true)]
        [InlineData(false, false, false)]
        [SqlServerCondition(SqlServerCondition.SupportsAttach)]
        public Task Deletes_database_with_filename(bool async, bool open, bool ambientTransaction)
        {
            return Delete_database_test(async, open, ambientTransaction, file: true);
        }

        private static async Task Delete_database_test(bool async, bool open, bool ambientTransaction, bool file)
        {
            using (var testDatabase = SqlServerTestStore.CreateInitialized("EnsureDeleteBlogging" + (file ? "File" : ""), file))
            {
                if (!open)
                {
                    testDatabase.CloseConnection();
                }

                using (var context = new SqlServerDatabaseCreatorTest.BloggingContext(testDatabase))
                {
                    var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(context);

                    Assert.True(async ? await creator.ExistsAsync() : creator.Exists());

                    using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                    {
                        if (async)
                        {
                            Assert.True(await context.Database.EnsureDeletedAsync());
                        }
                        else
                        {
                            Assert.True(context.Database.EnsureDeleted());
                        }
                    }

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);

                    Assert.False(async ? await creator.ExistsAsync() : creator.Exists());

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
                }
            }
        }

        [ConditionalTheory]
        [InlineData(true)]
        [InlineData(false)]
        public Task Noop_when_database_does_not_exist(bool async)
        {
            return Noop_when_database_does_not_exist_test(async, file: false);
        }

        [ConditionalTheory]
        [InlineData(true)]
        [InlineData(false)]
        [SqlServerCondition(SqlServerCondition.SupportsAttach)]
        public Task Noop_when_database_with_filename_does_not_exist(bool async)
        {
            return Noop_when_database_does_not_exist_test(async, file: true);
        }

        private static async Task Noop_when_database_does_not_exist_test(bool async, bool file)
        {
            using (var testDatabase = SqlServerTestStore.Create("NonExisting", file))
            {
                using (var context = new SqlServerDatabaseCreatorTest.BloggingContext(testDatabase))
                {
                    var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(context);

                    Assert.False(async ? await creator.ExistsAsync() : creator.Exists());

                    if (async)
                    {
                        Assert.False(await creator.EnsureDeletedAsync());
                    }
                    else
                    {
                        Assert.False(creator.EnsureDeleted());
                    }

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);

                    Assert.False(async ? await creator.ExistsAsync() : creator.Exists());

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
                }
            }
        }
    }

    public class SqlServerDatabaseCreatorEnsureCreatedTest
    {
        [ConditionalTheory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public Task Creates_schema_in_existing_database(bool async, bool ambientTransaction)
        {
            return Creates_schema_in_existing_database_test(async, ambientTransaction, file: false);
        }

        [ConditionalTheory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [SqlServerCondition(SqlServerCondition.SupportsAttach)]
        public Task Creates_schema_in_existing_database_with_filename(bool async, bool ambientTransaction)
        {
            return Creates_schema_in_existing_database_test(async, ambientTransaction, file: true);
        }

        private static Task Creates_schema_in_existing_database_test(bool async, bool ambientTransaction, bool file)
            => TestEnvironment.IsSqlAzure
                ? new TestSqlServerRetryingExecutionStrategy().ExecuteAsync(
                    (true, async, ambientTransaction, file), Creates_physical_database_and_schema_test)
                : Creates_physical_database_and_schema_test((true, async, ambientTransaction, file));

        [ConditionalTheory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [SqlServerCondition(SqlServerCondition.IsNotSqlAzure)]
        public Task Creates_physical_database_and_schema(bool async, bool ambientTransaction)
        {
            return Creates_new_physical_database_and_schema_test(async, ambientTransaction, file: false);
        }

        [ConditionalTheory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        [SqlServerCondition(SqlServerCondition.SupportsAttach)]
        public Task Creates_physical_database_with_filename_and_schema(bool async, bool ambientTransaction)
        {
            return Creates_new_physical_database_and_schema_test(async, ambientTransaction, file: true);
        }

        private static Task Creates_new_physical_database_and_schema_test(bool async, bool ambientTransaction, bool file)
            => TestEnvironment.IsSqlAzure
                ? new TestSqlServerRetryingExecutionStrategy().ExecuteAsync(
                    (false, async, ambientTransaction, file), Creates_physical_database_and_schema_test)
                : Creates_physical_database_and_schema_test((false, async, ambientTransaction, file));

        private static async Task Creates_physical_database_and_schema_test(
            (bool CreateDatabase, bool Async, bool ambientTransaction, bool File) options)
        {
            (bool createDatabase, bool async, bool ambientTransaction, bool file) = options;
            using (var testDatabase = SqlServerTestStore.Create("EnsureCreatedTest" + (file ? "File" : ""), file))
            {
                using (var context = new SqlServerDatabaseCreatorTest.BloggingContext(testDatabase))
                {
                    if (createDatabase)
                    {
                        testDatabase.Initialize(null, (Func<DbContext>)null, null);
                    }
                    else
                    {
                        testDatabase.DeleteDatabase();
                    }

                    var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(context);

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);

                    using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                    {
                        if (async)
                        {
                            Assert.True(await creator.EnsureCreatedAsync());
                        }
                        else
                        {
                            Assert.True(creator.EnsureCreated());
                        }
                    }

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);

                    if (testDatabase.ConnectionState != ConnectionState.Open)
                    {
                        await testDatabase.OpenConnectionAsync();
                    }

                    var tables = testDatabase.Query<string>(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'").ToList();
                    Assert.Equal(1, tables.Count);
                    Assert.Equal("Blogs", tables.Single());

                    var columns = testDatabase.Query<string>(
                        "SELECT TABLE_NAME + '.' + COLUMN_NAME + ' (' + DATA_TYPE + ')' FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = 'Blogs' ORDER BY TABLE_NAME, COLUMN_NAME").ToArray();
                    Assert.Equal(14, columns.Length);

                    Assert.Equal(
                        new[]
                        {
                            "Blogs.AndChew (varbinary)",
                            "Blogs.AndRow (timestamp)",
                            "Blogs.Cheese (nvarchar)",
                            "Blogs.ErMilan (int)",
                            "Blogs.Fuse (smallint)",
                            "Blogs.George (bit)",
                            "Blogs.Key1 (nvarchar)",
                            "Blogs.Key2 (varbinary)",
                            "Blogs.NotFigTime (datetime2)",
                            "Blogs.On (real)",
                            "Blogs.OrNothing (float)",
                            "Blogs.TheGu (uniqueidentifier)",
                            "Blogs.ToEat (tinyint)",
                            "Blogs.WayRound (bigint)"
                        },
                        columns);
                }
            }
        }

        [ConditionalTheory]
        [InlineData(true)]
        [InlineData(false)]
        public Task Noop_when_database_exists_and_has_schema(bool async)
        {
            return Noop_when_database_exists_and_has_schema_test(async, file: false);
        }

        [ConditionalTheory]
        [InlineData(true)]
        [InlineData(false)]
        [SqlServerCondition(SqlServerCondition.SupportsAttach)]
        public Task Noop_when_database_with_filename_exists_and_has_schema(bool async)
        {
            return Noop_when_database_exists_and_has_schema_test(async, file: true);
        }

        private static async Task Noop_when_database_exists_and_has_schema_test(bool async, bool file)
        {
            using (var testDatabase = SqlServerTestStore.CreateInitialized("InitializedBlogging" + (file ? "File" : ""), file))
            {
                using (var context = new SqlServerDatabaseCreatorTest.BloggingContext(testDatabase))
                {
                    context.Database.EnsureCreated();

                    if (async)
                    {
                        Assert.False(await context.Database.EnsureCreatedAsync());
                    }
                    else
                    {
                        Assert.False(context.Database.EnsureCreated());
                    }

                    Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
                }
            }
        }
    }

    public class SqlServerDatabaseCreatorHasTablesTest
    {
        [ConditionalTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Throws_when_database_does_not_exist(bool async)
        {
            using (var testDatabase = SqlServerTestStore.GetOrCreate("NonExisting"))
            {
                var databaseCreator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(testDatabase);
                await databaseCreator.ExecutionStrategyFactory.Create().ExecuteAsync(
                    databaseCreator,
                    async creator =>
                        {
                            var errorNumber = async
                                ? (await Assert.ThrowsAsync<SqlException>(() => creator.HasTablesAsyncBase())).Number
                                : Assert.Throws<SqlException>(() => creator.HasTablesBase()).Number;

                            if (errorNumber != 233) // skip if no-process transient failure
                            {
                                Assert.Equal(
                                    4060, // Login failed error number
                                    errorNumber);
                            }
                        });
            }
        }

        [ConditionalTheory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        public async Task Returns_false_when_database_exists_but_has_no_tables(bool async, bool ambientTransaction)
        {
            using (var testDatabase = SqlServerTestStore.GetOrCreateInitialized("Empty"))
            {
                var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(testDatabase);
                using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                {
                    Assert.False(async ? await creator.HasTablesAsyncBase() : creator.HasTablesBase());
                }
            }
        }

        [ConditionalTheory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public async Task Returns_true_when_database_exists_and_has_any_tables(bool async, bool ambientTransaction)
        {
            using (var testDatabase = SqlServerTestStore.GetOrCreate("ExistingTables")
                .InitializeSqlServer(null, t => new SqlServerDatabaseCreatorTest.BloggingContext(t), null))
            {
                var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(testDatabase);
                using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                {
                    Assert.True(async ? await creator.HasTablesAsyncBase() : creator.HasTablesBase());
                }
            }
        }
    }

    public class SqlServerDatabaseCreatorDeleteTest
    {
        [ConditionalTheory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public static async Task Deletes_database(bool async, bool ambientTransaction)
        {
            using (var testDatabase = SqlServerTestStore.CreateInitialized("DeleteBlogging"))
            {
                testDatabase.CloseConnection();

                var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(testDatabase);

                Assert.True(async ? await creator.ExistsAsync() : creator.Exists());

                using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                {
                    if (async)
                    {
                        await creator.DeleteAsync();
                    }
                    else
                    {
                        creator.Delete();
                    }
                }

                Assert.False(async ? await creator.ExistsAsync() : creator.Exists());
            }
        }

        [ConditionalTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Throws_when_database_does_not_exist(bool async)
        {
            using (var testDatabase = SqlServerTestStore.GetOrCreate("NonExistingBlogging"))
            {
                var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(testDatabase);

                if (async)
                {
                    await Assert.ThrowsAsync<SqlException>(() => creator.DeleteAsync());
                }
                else
                {
                    Assert.Throws<SqlException>(() => creator.Delete());
                }
            }
        }

        [ConditionalFact]
        public void Throws_when_no_initial_catalog()
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(TestEnvironment.DefaultConnection);
            connectionStringBuilder.Remove("Initial Catalog");

            var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(connectionStringBuilder.ToString());

            var ex = Assert.Throws<InvalidOperationException>(() => creator.Delete());

            Assert.Equal(SqlServerStrings.NoInitialCatalog, ex.Message);
        }
    }

    public class SqlServerDatabaseCreatorCreateTablesTest
    {
        [ConditionalTheory]
        [InlineData(true, true)]
        [InlineData(false, false)]
        public async Task Creates_schema_in_existing_database_test(bool async, bool ambientTransaction)
        {
            using (var testDatabase = SqlServerTestStore.GetOrCreateInitialized("ExistingBlogging" + (async ? "Async" : "")))
            {
                using (var context = new SqlServerDatabaseCreatorTest.BloggingContext(testDatabase))
                {
                    var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(context);

                    using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                    {
                        if (async)
                        {
                            await creator.CreateTablesAsync();
                        }
                        else
                        {
                            creator.CreateTables();
                        }
                    }

                    if (testDatabase.ConnectionState != ConnectionState.Open)
                    {
                        await testDatabase.OpenConnectionAsync();
                    }

                    var tables = (await testDatabase.QueryAsync<string>(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
                    Assert.Equal(1, tables.Count);
                    Assert.Equal("Blogs", tables.Single());

                    var columns = (await testDatabase.QueryAsync<string>(
                        "SELECT TABLE_NAME + '.' + COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Blogs'")).ToList();
                    Assert.Equal(14, columns.Count);
                    Assert.True(columns.Any(c => c == "Blogs.Key1"));
                    Assert.True(columns.Any(c => c == "Blogs.Key2"));
                    Assert.True(columns.Any(c => c == "Blogs.Cheese"));
                    Assert.True(columns.Any(c => c == "Blogs.ErMilan"));
                    Assert.True(columns.Any(c => c == "Blogs.George"));
                    Assert.True(columns.Any(c => c == "Blogs.TheGu"));
                    Assert.True(columns.Any(c => c == "Blogs.NotFigTime"));
                    Assert.True(columns.Any(c => c == "Blogs.ToEat"));
                    Assert.True(columns.Any(c => c == "Blogs.OrNothing"));
                    Assert.True(columns.Any(c => c == "Blogs.Fuse"));
                    Assert.True(columns.Any(c => c == "Blogs.WayRound"));
                    Assert.True(columns.Any(c => c == "Blogs.On"));
                    Assert.True(columns.Any(c => c == "Blogs.AndChew"));
                    Assert.True(columns.Any(c => c == "Blogs.AndRow"));
                }
            }
        }

        [ConditionalTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Throws_if_database_does_not_exist(bool async)
        {
            using (var testDatabase = SqlServerTestStore.GetOrCreate("NonExisting"))
            {
                var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(testDatabase);

                var errorNumber
                    = async
                        ? (await Assert.ThrowsAsync<SqlException>(() => creator.CreateTablesAsync())).Number
                        : Assert.Throws<SqlException>(() => creator.CreateTables()).Number;

                if (errorNumber != 233) // skip if no-process transient failure
                {
                    Assert.Equal(
                        4060, // Login failed error number
                        errorNumber);
                }
            }
        }
    }

    public class SqlServerDatabaseCreatorCreateTest
    {
        [ConditionalTheory]
        [InlineData(true, false)]
        [InlineData(false, true)]
        public async Task Creates_physical_database_but_not_tables(bool async, bool ambientTransaction)
        {
            using (var testDatabase = SqlServerTestStore.GetOrCreate("CreateTest"))
            {
                var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(testDatabase);

                creator.EnsureDeleted();

                using (SqlServerDatabaseCreatorTest.CreateTransaction(ambientTransaction))
                {
                    if (async)
                    {
                        await creator.CreateAsync();
                    }
                    else
                    {
                        creator.Create();
                    }
                }

                Assert.True(creator.Exists());

                if (testDatabase.ConnectionState != ConnectionState.Open)
                {
                    await testDatabase.OpenConnectionAsync();
                }

                Assert.Equal(
                    0, (await testDatabase.QueryAsync<string>(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")).Count());

                Assert.True(
                    await testDatabase.ExecuteScalarAsync<bool>(
                        string.Concat(
                            "SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name='",
                            testDatabase.Name,
                            "'")));
            }
        }

        [ConditionalTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Throws_if_database_already_exists(bool async)
        {
            using (var testDatabase = SqlServerTestStore.GetOrCreateInitialized("ExistingBlogging"))
            {
                var creator = SqlServerDatabaseCreatorTest.GetDatabaseCreator(testDatabase);

                var ex = async
                    ? await Assert.ThrowsAsync<SqlException>(() => creator.CreateAsync())
                    : Assert.Throws<SqlException>(() => creator.Create());
                Assert.Equal(
                    1801, // Database with given name already exists
                    ex.Number);
            }
        }
    }

    public class SqlServerDatabaseCreatorTest
    {
        public static TestDatabaseCreator GetDatabaseCreator(SqlServerTestStore testStore)
            => GetDatabaseCreator(testStore.ConnectionString);

        public static TestDatabaseCreator GetDatabaseCreator(string connectionString)
            => GetDatabaseCreator(new BloggingContext(connectionString));

        public static TestDatabaseCreator GetDatabaseCreator(BloggingContext context)
            => (TestDatabaseCreator)context.GetService<IRelationalDatabaseCreator>();

        // ReSharper disable once ClassNeverInstantiated.Local
        private class TestSqlServerExecutionStrategyFactory : SqlServerExecutionStrategyFactory
        {
            public TestSqlServerExecutionStrategyFactory(ExecutionStrategyDependencies dependencies)
                : base(dependencies)
            {
            }

            protected override IExecutionStrategy CreateDefaultStrategy(ExecutionStrategyDependencies dependencies)
                => new NoopExecutionStrategy(dependencies);
        }

        private static IServiceProvider CreateServiceProvider()
            => new ServiceCollection()
                .AddEntityFrameworkSqlServer()
                .AddScoped<IExecutionStrategyFactory, TestSqlServerExecutionStrategyFactory>()
                .AddScoped<IRelationalDatabaseCreator, TestDatabaseCreator>()
                .BuildServiceProvider();


        public static TransactionScope CreateTransaction(bool useTransaction)
        {
            if (useTransaction)
            {
#if NET461
                return new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
#endif
            }

            return new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        }

        public class BloggingContext : DbContext
        {
            private readonly string _connectionString;

            public BloggingContext(SqlServerTestStore testStore)
                : this(testStore.ConnectionString)
            {
            }

            public BloggingContext(string connectionString)
            {
                _connectionString = connectionString;
            }

            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
                => optionsBuilder
                    .UseSqlServer(_connectionString, b => b.ApplyConfiguration().CommandTimeout(SqlServerTestStore.CommandTimeout))
                    .UseInternalServiceProvider(CreateServiceProvider());

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                modelBuilder.Entity<Blog>(
                    b =>
                        {
                            b.HasKey(e => new { e.Key1, e.Key2 });
                            b.Property(e => e.AndRow).IsConcurrencyToken().ValueGeneratedOnAddOrUpdate();
                        });
            }

            public DbSet<Blog> Blogs { get; set; }
        }

        public class Blog
        {
            public string Key1 { get; set; }
            public byte[] Key2 { get; set; }
            public string Cheese { get; set; }
            public int ErMilan { get; set; }
            public bool George { get; set; }
            public Guid TheGu { get; set; }
            public DateTime NotFigTime { get; set; }
            public byte ToEat { get; set; }
            public double OrNothing { get; set; }
            public short Fuse { get; set; }
            public long WayRound { get; set; }
            public float On { get; set; }
            public byte[] AndChew { get; set; }
            public byte[] AndRow { get; set; }
        }

        public class TestDatabaseCreator : SqlServerDatabaseCreator
        {
            public TestDatabaseCreator(
                RelationalDatabaseCreatorDependencies dependencies,
                ISqlServerConnection connection,
                IRawSqlCommandBuilder rawSqlCommandBuilder)
                : base(dependencies, connection, rawSqlCommandBuilder)
            {
            }

            public bool HasTablesBase() => HasTables();

            public Task<bool> HasTablesAsyncBase(CancellationToken cancellationToken = default(CancellationToken))
                => HasTablesAsync(cancellationToken);

            public IExecutionStrategyFactory ExecutionStrategyFactory => Dependencies.ExecutionStrategyFactory;
        }
    }
}
