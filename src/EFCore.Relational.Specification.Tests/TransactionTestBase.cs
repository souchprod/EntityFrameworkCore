// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

// ReSharper disable InconsistentNaming
namespace Microsoft.EntityFrameworkCore
{
    public abstract class TransactionTestBase<TFixture> : IClassFixture<TFixture>
        where TFixture : TransactionTestBase<TFixture>.TransactionFixtureBase, new()
    {
        protected TransactionTestBase(TFixture fixture)
        {
            Fixture = fixture;
            Fixture.Reseed();
        }

        protected TFixture Fixture { get; set; }

        [Fact]
        public virtual void SaveChanges_can_be_used_with_no_transaction()
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = false;

                context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                context.Entry(context.Set<TransactionCustomer>().Last()).State = EntityState.Added;

                Assert.Throws<DbUpdateException>(() => context.SaveChanges());
            }

            using (var context = CreateContext())
            {
                Assert.Equal(
                    new List<int> { 1, 2, 77 },
                    context.Set<TransactionCustomer>().OrderBy(c => c.Id).Select(e => e.Id).ToList());
            }
        }

        [Fact]
        public virtual async Task SaveChangesAsync_can_be_used_with_no_transaction()
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = false;

                context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                context.Entry(context.Set<TransactionCustomer>().Last()).State = EntityState.Added;

                try
                {
                    await context.SaveChangesAsync();
                }
                catch (DbUpdateException)
                {
                }
            }

            using (var context = CreateContext())
            {
                Assert.Equal(
                    new List<int> { 1, 2, 77 },
                    context.Set<TransactionCustomer>().OrderBy(c => c.Id).Select(e => e.Id).ToList());
            }
        }

        [Fact]
        public virtual void SaveChanges_implicitly_starts_transaction()
        {
            using (var context = CreateContext())
            {
                context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                context.Entry(context.Set<TransactionCustomer>().Last()).State = EntityState.Added;

                Assert.Throws<DbUpdateException>(() => context.SaveChanges());
            }

            AssertStoreInitialState();
        }

        [Fact]
        public virtual async Task SaveChangesAsync_implicitly_starts_transaction()
        {
            using (var context = CreateContext())
            {
                context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                context.Entry(context.Set<TransactionCustomer>().Last()).State = EntityState.Added;

                try
                {
                    await context.SaveChangesAsync();
                }
                catch (DbUpdateException)
                {
                }
            }

            AssertStoreInitialState();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual void SaveChanges_uses_enlisted_transaction(bool autoTransactionsEnabled)
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (var transaction = new CommittableTransaction())
            {
                using (var context = CreateContext())
                {
                    context.Database.EnlistTransaction(transaction);
                    context.Database.AutoTransactionsEnabled = autoTransactionsEnabled;

                    context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                    context.Entry(context.Set<TransactionCustomer>().Last()).State = EntityState.Added;

                    Assert.Throws<DbUpdateException>(() => context.SaveChanges());
                }

                using (var context = CreateContext())
                {
                    Assert.NotNull(context.Set<TransactionCustomer>().Find(77));
                }
            }

            AssertStoreInitialState();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task SaveChangesAsync_uses_enlisted_transaction(bool autoTransactionsEnabled)
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (var transaction = new CommittableTransaction())
            {
                using (var context = CreateContext())
                {
                    context.Database.EnlistTransaction(transaction);
                    context.Database.AutoTransactionsEnabled = autoTransactionsEnabled;

                    context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                    context.Entry(context.Set<TransactionCustomer>().Last()).State = EntityState.Added;

                    try
                    {
                        await context.SaveChangesAsync();
                    }
                    catch (DbUpdateException)
                    {
                    }
                }

                using (var context = CreateContext())
                {
                    Assert.NotNull(await context.Set<TransactionCustomer>().FindAsync( 77 ));
                }
            }

            AssertStoreInitialState();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual void SaveChanges_uses_ambient_transaction(bool autoTransactionsEnabled)
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var context = CreateContext())
                {
                    context.Database.AutoTransactionsEnabled = autoTransactionsEnabled;

                    context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                    context.Entry(context.Set<TransactionCustomer>().Last()).State = EntityState.Added;

                    Assert.Throws<DbUpdateException>(() => context.SaveChanges());
                }
            }

            AssertStoreInitialState();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task SaveChangesAsync_uses_ambient_transaction(bool autoTransactionsEnabled)
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var context = CreateContext())
                {
                    context.Database.AutoTransactionsEnabled = autoTransactionsEnabled;

                    context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                    context.Entry(context.Set<TransactionCustomer>().Last()).State = EntityState.Added;

                    try
                    {
                        await context.SaveChangesAsync();
                    }
                    catch (DbUpdateException)
                    {
                    }
                }
            }

            AssertStoreInitialState();
        }

        [Fact]
        public virtual void SaveChanges_does_not_close_connection_opened_by_user()
        {
            using (var context = CreateContext())
            {
                var connection = context.Database.GetDbConnection();
                context.Database.OpenConnection();

                Assert.Equal(ConnectionState.Open, connection.State);

                context.Database.AutoTransactionsEnabled = true;

                context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                context.SaveChanges();

                Assert.Equal(ConnectionState.Open, connection.State);
            }

            using (var context = CreateContext())
            {
                Assert.Equal(
                    new List<int> { 1, 2, 77 },
                    context.Set<TransactionCustomer>().OrderBy(c => c.Id).Select(e => e.Id).ToList());
            }
        }

        [Fact]
        public virtual async Task SaveChangesAsync_does_not_close_connection_opened_by_user()
        {
            using (var context = CreateContext())
            {
                var connection = context.Database.GetDbConnection();
                context.Database.OpenConnection();

                Assert.Equal(ConnectionState.Open, connection.State);

                context.Database.AutoTransactionsEnabled = true;

                context.Add(new TransactionCustomer { Id = 77, Name = "Bobble" });
                await context.SaveChangesAsync();

                Assert.Equal(ConnectionState.Open, connection.State);
            }

            using (var context = CreateContext())
            {
                Assert.Equal(
                    new List<int> { 1, 2, 77 },
                    context.Set<TransactionCustomer>().OrderBy(c => c.Id).Select(e => e.Id).ToList());
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual void SaveChanges_uses_explicit_transaction_without_committing(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                var firstEntry = context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First());
                firstEntry.State = EntityState.Deleted;

                using (context.Database.BeginTransaction())
                {
                    context.SaveChanges();
                }

                Assert.Equal(EntityState.Detached, firstEntry.State);
            }

            AssertStoreInitialState();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual void SaveChanges_false_uses_explicit_transaction_without_committing_or_accepting_changes(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                var firstEntry = context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First());
                firstEntry.State = EntityState.Deleted;

                using (context.Database.BeginTransaction())
                {
                    context.SaveChanges(acceptAllChangesOnSuccess: false);
                }

                Assert.Equal(EntityState.Deleted, firstEntry.State);

                context.ChangeTracker.AcceptAllChanges();

                Assert.Equal(EntityState.Detached, firstEntry.State);
            }

            AssertStoreInitialState();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task SaveChangesAsync_uses_explicit_transaction_without_committing(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                var firstEntry = context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First());
                firstEntry.State = EntityState.Deleted;

                using (await context.Database.BeginTransactionAsync())
                {
                    await context.SaveChangesAsync();
                }

                Assert.Equal(EntityState.Detached, firstEntry.State);
            }

            AssertStoreInitialState();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task SaveChangesAsync_false_uses_explicit_transaction_without_committing_or_accepting_changes(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                var firstEntry = context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First());
                firstEntry.State = EntityState.Deleted;

                using (await context.Database.BeginTransactionAsync())
                {
                    await context.SaveChangesAsync(acceptAllChangesOnSuccess: false);
                }

                Assert.Equal(EntityState.Deleted, firstEntry.State);

                context.ChangeTracker.AcceptAllChanges();

                Assert.Equal(EntityState.Detached, firstEntry.State);
            }

            AssertStoreInitialState();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual void SaveChanges_uses_explicit_transaction_and_does_not_rollback_on_failure(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                using (var transaction = context.Database.BeginTransaction())
                {
                    var firstEntry = context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First());
                    firstEntry.State = EntityState.Deleted;
                    var lastEntry = context.Entry(context.Set<TransactionCustomer>().Last());
                    lastEntry.State = EntityState.Added;

                    try
                    {
                        context.SaveChanges();
                    }
                    catch (DbUpdateException)
                    {
                    }

                    Assert.Equal(EntityState.Deleted, firstEntry.State);
                    Assert.Equal(EntityState.Added, lastEntry.State);
                    Assert.NotNull(transaction.GetDbTransaction().Connection);
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task SaveChangesAsync_uses_explicit_transaction_and_does_not_rollback_on_failure(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                using (var transaction = await context.Database.BeginTransactionAsync())
                {
                    var firstEntry = context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First());
                    firstEntry.State = EntityState.Deleted;
                    var lastEntry = context.Entry(context.Set<TransactionCustomer>().Last());
                    lastEntry.State = EntityState.Added;

                    try
                    {
                        await context.SaveChangesAsync();
                    }
                    catch (DbUpdateException)
                    {
                    }

                    Assert.Equal(EntityState.Deleted, firstEntry.State);
                    Assert.Equal(EntityState.Added, lastEntry.State);
                    Assert.NotNull(transaction.GetDbTransaction().Connection);
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task RelationalTransaction_can_be_commited(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                using (var transaction = await context.Database.BeginTransactionAsync())
                {
                    context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First()).State = EntityState.Deleted;
                    await context.SaveChangesAsync();
                    transaction.Commit();
                }
            }

            using (var context = CreateContext())
            {
                Assert.Equal(Customers.Count - 1, context.Set<TransactionCustomer>().Count());
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task RelationalTransaction_can_be_commited_from_context(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                using (await context.Database.BeginTransactionAsync())
                {
                    context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First()).State = EntityState.Deleted;
                    await context.SaveChangesAsync();
                    context.Database.CommitTransaction();
                }
            }

            using (var context = CreateContext())
            {
                Assert.Equal(Customers.Count - 1, context.Set<TransactionCustomer>().Count());
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task RelationalTransaction_can_be_rolled_back(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                using (var transaction = await context.Database.BeginTransactionAsync())
                {
                    context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First()).State = EntityState.Deleted;
                    await context.SaveChangesAsync();
                    transaction.Rollback();

                    AssertStoreInitialState();
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task RelationalTransaction_can_be_rolled_back_from_context(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                using (await context.Database.BeginTransactionAsync())
                {
                    context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First()).State = EntityState.Deleted;
                    await context.SaveChangesAsync();
                    context.Database.RollbackTransaction();

                    AssertStoreInitialState();
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual void Query_uses_explicit_transaction(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                using (var transaction = context.Database.BeginTransaction())
                {
                    context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First()).State = EntityState.Deleted;
                    context.SaveChanges();

                    using (var innerContext = CreateContextWithConnectionString())
                    {
                        innerContext.Database.AutoTransactionsEnabled = autoTransaction;

                        if (DirtyReadsOccur)
                        {
                            // ReSharper disable once RedundantNameQualifier
                            using (innerContext.Database.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
                            {
                                Assert.Equal(Customers.Count - 1, innerContext.Set<TransactionCustomer>().Count());
                            }
                        }

                        if (SnapshotSupported)
                        {
                            // ReSharper disable once RedundantNameQualifier
                            using (innerContext.Database.BeginTransaction(System.Data.IsolationLevel.Snapshot))
                            {
                                Assert.Equal(Customers, innerContext.Set<TransactionCustomer>().OrderBy(c => c.Id).ToList());
                            }
                        }
                    }

                    using (var innerContext = CreateContext())
                    {
                        innerContext.Database.AutoTransactionsEnabled = autoTransaction;

                        innerContext.Database.UseTransaction(transaction.GetDbTransaction());
                        Assert.Equal(Customers.Count - 1, innerContext.Set<TransactionCustomer>().Count());
                    }
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task QueryAsync_uses_explicit_transaction(bool autoTransaction)
        {
            using (var context = CreateContext())
            {
                context.Database.AutoTransactionsEnabled = autoTransaction;

                using (var transaction = await context.Database.BeginTransactionAsync())
                {
                    context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First()).State = EntityState.Deleted;
                    await context.SaveChangesAsync();

                    using (var innerContext = CreateContextWithConnectionString())
                    {
                        innerContext.Database.AutoTransactionsEnabled = autoTransaction;

                        if (DirtyReadsOccur)
                        {
                            // ReSharper disable once RedundantNameQualifier
                            using (await innerContext.Database.BeginTransactionAsync(System.Data.IsolationLevel.ReadUncommitted))
                            {
                                Assert.Equal(Customers.Count - 1, await innerContext.Set<TransactionCustomer>().CountAsync());
                            }
                        }

                        if (SnapshotSupported)
                        {
                            // ReSharper disable once RedundantNameQualifier
                            using (await innerContext.Database.BeginTransactionAsync(System.Data.IsolationLevel.Snapshot))
                            {
                                Assert.Equal(Customers, await innerContext.Set<TransactionCustomer>().OrderBy(c => c.Id).ToListAsync());
                            }
                        }
                    }

                    using (var innerContext = CreateContext())
                    {
                        innerContext.Database.AutoTransactionsEnabled = autoTransaction;

                        innerContext.Database.UseTransaction(transaction.GetDbTransaction());
                        Assert.Equal(Customers.Count - 1, await innerContext.Set<TransactionCustomer>().CountAsync());
                    }
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public virtual async Task Can_use_open_connection_with_started_transaction(bool autoTransaction)
        {
            using (var transaction = TestStore.BeginTransaction())
            {
                using (var context = CreateContext())
                {
                    context.Database.AutoTransactionsEnabled = autoTransaction;

                    context.Database.UseTransaction(transaction);

                    context.Entry(context.Set<TransactionCustomer>().OrderBy(c => c.Id).First()).State = EntityState.Deleted;
                    await context.SaveChangesAsync();
                }
            }

            AssertStoreInitialState();
        }

        [Fact]
        public virtual void UseTransaction_throws_if_mismatched_connection()
        {
            using (var transaction = TestStore.BeginTransaction())
            {
                using (var context = CreateContextWithConnectionString())
                {
                    var ex = Assert.Throws<InvalidOperationException>(
                        () =>
                            context.Database.UseTransaction(transaction));
                    Assert.Equal(RelationalStrings.TransactionAssociatedWithDifferentConnection, ex.Message);
                }
            }
        }

        [Fact]
        public virtual void UseTransaction_throws_if_another_transaction_started()
        {
            using (var transaction = TestStore.BeginTransaction())
            {
                using (var context = CreateContextWithConnectionString())
                {
                    using (context.Database.BeginTransaction())
                    {
                        var ex = Assert.Throws<InvalidOperationException>(
                            () =>
                                context.Database.UseTransaction(transaction));
                        Assert.Equal(RelationalStrings.TransactionAlreadyStarted, ex.Message);
                    }
                }
            }
        }

        [Fact]
        public virtual void UseTransaction_throws_if_ambient_transaction_started()
        {
            using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var transaction = TestStore.BeginTransaction())
                {
                    using (var context = CreateContextWithConnectionString())
                    {
                        var ex = Assert.Throws<InvalidOperationException>(
                            () => context.Database.UseTransaction(transaction));
                        Assert.Equal(RelationalStrings.ConflictingAmbientTransaction, ex.Message);
                    }
                }
            }
        }

        [Fact]
        public virtual void UseTransaction_will_not_dispose_external_transaction()
        {
            using (var transaction = TestStore.BeginTransaction())
            {
                using (var context = CreateContext())
                {
                    context.Database.UseTransaction(transaction);

                    context.Database.GetService<IRelationalConnection>().Dispose();

                    Assert.NotNull(transaction.Connection);
                }
            }
        }

        [Fact]
        public virtual void UseTransaction_throws_if_enlisted_in_transaction()
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }
            using (var t = new CommittableTransaction())
            {
                using (var transaction = TestStore.BeginTransaction())
                {
                    using (var context = CreateContextWithConnectionString())
                    {
                        context.Database.OpenConnection();

                        context.Database.EnlistTransaction(t);

                        var ex = Assert.Throws<InvalidOperationException>(
                            () => context.Database.UseTransaction(transaction));
                        Assert.Equal(RelationalStrings.ConflictingEnlistedTransaction, ex.Message);
                    }
                }
            }
        }

        [Fact]
        public virtual void BeginTransaction_throws_if_another_transaction_started()
        {
            using (var context = CreateContextWithConnectionString())
            {
                using (context.Database.BeginTransaction())
                {
                    var ex = Assert.Throws<InvalidOperationException>(
                        () => context.Database.BeginTransaction());
                    Assert.Equal(RelationalStrings.TransactionAlreadyStarted, ex.Message);
                }
            }
        }

        [Fact]
        public virtual void BeginTransaction_throws_if_ambient_transaction_started()
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var context = CreateContextWithConnectionString())
                {
                    var ex = Assert.Throws<InvalidOperationException>(
                        () => context.Database.BeginTransaction());
                    Assert.Equal(RelationalStrings.ConflictingAmbientTransaction, ex.Message);
                }
            }
        }

        [Fact]
        public virtual void BeginTransaction_throws_if_enlisted_in_transaction()
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (var transaction = new CommittableTransaction())
            {
                using (var context = CreateContextWithConnectionString())
                {
                    context.Database.OpenConnection();

                    context.Database.EnlistTransaction(transaction);

                    var ex = Assert.Throws<InvalidOperationException>(
                        () => context.Database.BeginTransaction());
                    Assert.Equal(RelationalStrings.ConflictingEnlistedTransaction, ex.Message);
                }
            }
        }

        [Fact]
        public virtual void BeginTransaction_can_be_used_after_ambient_transaction_ended()
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (var context = CreateContextWithConnectionString())
            {
                using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {
                    context.Database.OpenConnection();
                }

                using (context.Database.BeginTransaction())
                {
                }
            }
        }

        [Fact]
        public virtual void BeginTransaction_can_be_used_after_enlisted_transaction_ended()
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (var context = CreateContextWithConnectionString())
            {
                using (var transaction = new CommittableTransaction())
                {
                    context.Database.OpenConnection();

                    context.Database.EnlistTransaction(transaction);
                }

                using (context.Database.BeginTransaction())
                {
                }
            }
        }

        [Fact]
        public virtual void BeginTransaction_can_be_used_after_another_transaction_connection_closed()
        {
            using (var context = CreateContextWithConnectionString())
            {
                using (context.Database.BeginTransaction())
                {
                    context.Database.CloseConnection();
                    using (context.Database.BeginTransaction())
                    {
                    }
                }
            }
        }

        [Fact]
        public virtual void BeginTransaction_can_be_used_after_enlisted_transaction_connection_closed()
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (var context = CreateContextWithConnectionString())
            {
                using (var transaction = new CommittableTransaction())
                {
                    context.Database.OpenConnection();

                    context.Database.EnlistTransaction(transaction);

                    context.Database.CloseConnection();

                    using (context.Database.BeginTransaction())
                    {
                    }
                }
            }
        }

        [Fact]
        public virtual void EnlistTransaction_throws_if_another_transaction_started()
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (var transaction = new CommittableTransaction())
            {
                using (var context = CreateContextWithConnectionString())
                {
                    using (context.Database.BeginTransaction())
                    {
                        Assert.Throws<InvalidOperationException>(
                            () => context.Database.EnlistTransaction(transaction));
                    }
                }
            }
        }

        [Fact]
        public virtual void EnlistTransaction_throws_if_ambient_transaction_started()
        {
            if (!AmbientTransactionsSupported)
            {
                return;
            }

            using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var transaction = new CommittableTransaction())
                {
                    using (var context = CreateContextWithConnectionString())
                    {
                        context.Database.OpenConnection();

                        Assert.Throws<InvalidOperationException>(
                            () => context.Database.EnlistTransaction(transaction));
                    }
                }
            }
        }

        protected virtual void AssertStoreInitialState()
        {
            using (var context = CreateContext())
            {
                Assert.Equal(Customers, context.Set<TransactionCustomer>().OrderBy(c => c.Id));
            }
        }

        protected RelationalTestStore TestStore => (RelationalTestStore)Fixture.TestStore;

        protected abstract bool SnapshotSupported { get; }

        protected virtual bool AmbientTransactionsSupported => false;

        protected virtual bool DirtyReadsOccur => true;

        protected DbContext CreateContext() => Fixture.CreateContext();

        protected abstract DbContext CreateContextWithConnectionString();

        public abstract class TransactionFixtureBase : SharedStoreFixtureBase<DbContext>
        {
            protected override string StoreName { get; } = "TransactionTest";
            protected override bool UsePooling => false;

            protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
            {
                modelBuilder.Entity<TransactionCustomer>(
                    ps =>
                        {
                            ps.Property(c => c.Id).ValueGeneratedNever();
                            ps.ToTable("Customers");
                        });
            }

            protected override void Seed(DbContext context)
            {
                context.AddRange(Customers);
                context.SaveChanges();
            }
        }

        protected static readonly IReadOnlyList<TransactionCustomer> Customers = new List<TransactionCustomer>
        {
            new TransactionCustomer
            {
                Id = 1,
                Name = "Bob"
            },
            new TransactionCustomer
            {
                Id = 2,
                Name = "Dave"
            }
        };

        protected class TransactionCustomer
        {
            public int Id { get; set; }
            public string Name { get; set; }

            public override bool Equals(object obj)
            {
                var otherCustomer = obj as TransactionCustomer;
                if (otherCustomer == null)
                {
                    return false;
                }

                return Id == otherCustomer.Id
                       && Name == otherCustomer.Name;
            }

            public override string ToString() => "Id = " + Id + ", Name = " + Name;

            // ReSharper disable NonReadonlyMemberInGetHashCode
            public override int GetHashCode() => Id.GetHashCode() * 397 ^ Name.GetHashCode();
        }
    }
}
