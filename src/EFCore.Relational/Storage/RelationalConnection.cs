// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Utilities;
using IsolationLevel = System.Data.IsolationLevel;

namespace Microsoft.EntityFrameworkCore.Storage
{
    /// <summary>
    ///     <para>
    ///         Represents a connection with a relational database.
    ///     </para>
    ///     <para>
    ///         This type is typically used by database providers (and other extensions). It is generally
    ///         not used in application code.
    ///     </para>
    /// </summary>
    public abstract class RelationalConnection : IRelationalConnection
    {
        private readonly string _connectionString;
        private readonly LazyRef<DbConnection> _connection;
        private readonly bool _connectionOwned;
        private int _openedCount;
        private bool _openedInternally;
        private int? _commandTimeout;

        /// <summary>
        ///     Initializes a new instance of the <see cref="RelationalConnection" /> class.
        /// </summary>
        /// <param name="dependencies"> Parameter object containing dependencies for this service. </param>
        protected RelationalConnection([NotNull] RelationalConnectionDependencies dependencies)
        {
            Check.NotNull(dependencies, nameof(dependencies));

            Dependencies = dependencies;

            var relationalOptions = RelationalOptionsExtension.Extract(dependencies.ContextOptions);

            _commandTimeout = relationalOptions.CommandTimeout;

            if (relationalOptions.Connection != null)
            {
                if (!string.IsNullOrWhiteSpace(relationalOptions.ConnectionString))
                {
                    throw new InvalidOperationException(RelationalStrings.ConnectionAndConnectionString);
                }

                _connection = new LazyRef<DbConnection>(() => relationalOptions.Connection);
                _connectionOwned = false;
            }
            else if (!string.IsNullOrWhiteSpace(relationalOptions.ConnectionString))
            {
                _connectionString = dependencies.ConnectionStringResolver.ResolveConnectionString(relationalOptions.ConnectionString);
                _connection = new LazyRef<DbConnection>(CreateDbConnection);
                _connectionOwned = true;
            }
            else
            {
                throw new InvalidOperationException(RelationalStrings.NoConnectionOrConnectionString);
            }
        }

        /// <summary>
        ///     The unique identifier for this connection.
        /// </summary>
        public virtual Guid ConnectionId { get; } = Guid.NewGuid();

        /// <summary>
        ///     Parameter object containing service dependencies.
        /// </summary>
        protected virtual RelationalConnectionDependencies Dependencies { get; }

        /// <summary>
        ///     Creates a <see cref="DbConnection" /> to the database.
        /// </summary>
        /// <returns> The connection. </returns>
        protected abstract DbConnection CreateDbConnection();

        /// <summary>
        ///     Gets the connection string for the database.
        /// </summary>
        public virtual string ConnectionString => _connectionString ?? DbConnection.ConnectionString;

        /// <summary>
        ///     Gets the underlying <see cref="System.Data.Common.DbConnection" /> used to connect to the database.
        /// </summary>
        public virtual DbConnection DbConnection => _connection.Value;

        /// <summary>
        ///     Gets the current transaction.
        /// </summary>
        public virtual IDbContextTransaction CurrentTransaction { get; [param: CanBeNull] protected set; }

        /// <summary>
        ///     The currently enlisted transaction.
        /// </summary>
        public virtual Transaction EnlistedTransaction
        {
            get
            {
                if (_enlistedTransaction != null)
                {
                    try
                    {
                        if (_enlistedTransaction.TransactionInformation.Status != TransactionStatus.Active)
                        {
                            _enlistedTransaction = null;
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        _enlistedTransaction = null;
                    }
                }
                return _enlistedTransaction;
            }
            [param: CanBeNull] protected set { _enlistedTransaction = value; }
        }

        /// <summary>
        ///     Specifies an existing <see cref="Transaction" /> to be used for database operations.
        /// </summary>
        /// <param name="transaction"> The transaction to be used. </param>
        public virtual void EnlistTransaction(Transaction transaction)
        {
            DbConnection.EnlistTransaction(transaction);

            EnlistedTransaction = transaction;
        }

        /// <summary>
        ///     The last ambient transaction used.
        /// </summary>
        public virtual Transaction AmbientTransaction { get; [param: CanBeNull] protected set; }

        /// <summary>
        ///     Indicates whether the store connection supports ambient transactions
        /// </summary>
        protected virtual bool SupportsAmbientTransactions => false;

        /// <summary>
        ///     Gets the timeout for executing a command against the database.
        /// </summary>
        public virtual int? CommandTimeout
        {
            get => _commandTimeout;
            set
            {
                if (value.HasValue
                    && value < 0)
                {
                    throw new ArgumentException(RelationalStrings.InvalidCommandTimeout);
                }

                _commandTimeout = value;
            }
        }

        /// <summary>
        ///     Begins a new transaction.
        /// </summary>
        /// <returns> The newly created transaction. </returns>
        [NotNull]
        public virtual IDbContextTransaction BeginTransaction() => BeginTransaction(IsolationLevel.Unspecified);

        /// <summary>
        ///     Asynchronously begins a new transaction.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> to observe while waiting for the task to complete.</param>
        /// <returns>
        ///     A task that represents the asynchronous operation. The task result contains the newly created transaction.
        /// </returns>
        [NotNull]
        public virtual async Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default(CancellationToken))
            => await BeginTransactionAsync(IsolationLevel.Unspecified, cancellationToken);

        /// <summary>
        ///     Begins a new transaction.
        /// </summary>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <returns> The newly created transaction. </returns>
        [NotNull]
        public virtual IDbContextTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            Open();

            EnsureNoTransactions();

            return BeginTransactionWithNoPreconditions(isolationLevel);
        }

        /// <summary>
        ///     Asynchronously begins a new transaction.
        /// </summary>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> to observe while waiting for the task to complete.</param>
        /// <returns>
        ///     A task that represents the asynchronous operation. The task result contains the newly created transaction.
        /// </returns>
        [NotNull]
        public virtual async Task<IDbContextTransaction> BeginTransactionAsync(
            IsolationLevel isolationLevel,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            await OpenAsync(cancellationToken);

            EnsureNoTransactions();

            return BeginTransactionWithNoPreconditions(isolationLevel);
        }

        private void EnsureNoTransactions()
        {
            if (CurrentTransaction != null)
            {
                throw new InvalidOperationException(RelationalStrings.TransactionAlreadyStarted);
            }

            if (Transaction.Current != null)
            {
                throw new InvalidOperationException(RelationalStrings.ConflictingAmbientTransaction);
            }

            if (EnlistedTransaction != null)
            {
                throw new InvalidOperationException(RelationalStrings.ConflictingEnlistedTransaction);
            }
        }

        private IDbContextTransaction BeginTransactionWithNoPreconditions(IsolationLevel isolationLevel)
        {
            var dbTransaction = DbConnection.BeginTransaction(isolationLevel);

            CurrentTransaction
                = new RelationalTransaction(
                    this,
                    dbTransaction,
                    Dependencies.TransactionLogger,
                    transactionOwned: true);

            Dependencies.TransactionLogger.TransactionStarted(
                this,
                dbTransaction,
                CurrentTransaction.TransactionId,
                DateTimeOffset.UtcNow);

            return CurrentTransaction;
        }

        /// <summary>
        ///     Specifies an existing <see cref="DbTransaction" /> to be used for database operations.
        /// </summary>
        /// <param name="transaction"> The transaction to be used. </param>
        public virtual IDbContextTransaction UseTransaction(DbTransaction transaction)
        {
            if (transaction == null)
            {
                if (CurrentTransaction != null)
                {
                    CurrentTransaction = null;
                }
            }
            else
            {
                EnsureNoTransactions();

                Open();

                CurrentTransaction = new RelationalTransaction(
                    this,
                    transaction,
                    Dependencies.TransactionLogger,
                    transactionOwned: false);

                Dependencies.TransactionLogger.TransactionUsed(
                    this,
                    transaction,
                    CurrentTransaction.TransactionId,
                    DateTimeOffset.UtcNow);
            }

            return CurrentTransaction;
        }

        /// <summary>
        ///     Commits all changes made to the database in the current transaction.
        /// </summary>
        public virtual void CommitTransaction()
        {
            if (CurrentTransaction == null)
            {
                throw new InvalidOperationException(RelationalStrings.NoActiveTransaction);
            }

            CurrentTransaction.Commit();
        }

        /// <summary>
        ///     Discards all changes made to the database in the current transaction.
        /// </summary>
        public virtual void RollbackTransaction()
        {
            if (CurrentTransaction == null)
            {
                throw new InvalidOperationException(RelationalStrings.NoActiveTransaction);
            }

            CurrentTransaction.Rollback();
        }

        /// <summary>
        ///     Opens the connection to the database.
        /// </summary>
        /// <param name="errorsExpected">
        ///     Indicates if the connection errors are expected and should be logged as debug message.
        /// </param>
        /// <returns> True if the underlying connection was actually opened; false otherwise. </returns>
        public virtual bool Open(bool errorsExpected = false)
        {
            if (DbConnection.State == ConnectionState.Broken)
            {
                DbConnection.Close();
            }

            var wasOpened = false;

            if (DbConnection.State != ConnectionState.Open)
            {
                OpenDbConnection(errorsExpected);
                wasOpened = true;
                CurrentTransaction = null;
                EnlistedTransaction = null;
            }
            else
            {
                _openedCount++;
            }

            HandleAmbientTransactions(() => OpenDbConnection(errorsExpected));

            return wasOpened;
        }

        /// <summary>
        ///     Asynchronously opens the connection to the database.
        /// </summary>
        /// <param name="errorsExpected"> Indicate if the connection errors are expected and should be logged as debug message. </param>
        /// <param name="cancellationToken">
        ///     A <see cref="CancellationToken" /> to observe while waiting for the task to complete.
        /// </param>
        /// <returns>
        ///     A task that represents the asynchronous operation, with a value of true if the connection
        ///     was actually opened.
        /// </returns>
        public virtual async Task<bool> OpenAsync(CancellationToken cancellationToken, bool errorsExpected = false)
        {
            if (DbConnection.State == ConnectionState.Broken)
            {
                DbConnection.Close();
            }

            var wasOpened = false;

            if (DbConnection.State != ConnectionState.Open)
            {
                await OpenDbConnectionAsync(errorsExpected, cancellationToken);
                wasOpened = true;
                CurrentTransaction = null;
                EnlistedTransaction = null;
            }
            else
            {
                _openedCount++;
            }

            await HandleAmbientTransactionsAsync(() => OpenDbConnectionAsync(errorsExpected, cancellationToken), cancellationToken);

            return wasOpened;
        }

        private void OpenDbConnection(bool errorsExpected)
        {
            var startTime = DateTimeOffset.UtcNow;
            var stopwatch = Stopwatch.StartNew();

            Dependencies.ConnectionLogger.ConnectionOpening(
                this,
                startTime,
                async: false);

            try
            {
                DbConnection.Open();

                Dependencies.ConnectionLogger.ConnectionOpened(
                    this,
                    startTime,
                    stopwatch.Elapsed,
                    async: false);
            }
            catch (Exception e)
            {
                Dependencies.ConnectionLogger.ConnectionError(
                    this,
                    e,
                    startTime,
                    stopwatch.Elapsed,
                    async: false,
                    logErrorAsDebug: errorsExpected);

                throw;
            }

            if (_openedCount == 0)
            {
                _openedInternally = true;
                _openedCount++;
            }
        }

        private async Task OpenDbConnectionAsync(bool errorsExpected, CancellationToken cancellationToken)
        {
            var startTime = DateTimeOffset.UtcNow;
            var stopwatch = Stopwatch.StartNew();

            Dependencies.ConnectionLogger.ConnectionOpening(
                this,
                startTime,
                async: true);

            try
            {
                await DbConnection.OpenAsync(cancellationToken);

                Dependencies.ConnectionLogger.ConnectionOpened(
                    this,
                    startTime,
                    stopwatch.Elapsed,
                    async: true);
            }
            catch (Exception e)
            {
                Dependencies.ConnectionLogger.ConnectionError(
                    this,
                    e,
                    startTime,
                    stopwatch.Elapsed,
                    async: true,
                    logErrorAsDebug: errorsExpected);

                throw;
            }

            if (_openedCount == 0)
            {
                _openedInternally = true;
                _openedCount++;
            }
        }

        /// <summary>
        ///     Ensures the connection is enlisted in the ambient transaction if present.
        /// </summary>
        /// <param name="open"> A delegate to open the underlying connection. </param>
        protected virtual void HandleAmbientTransactions([NotNull] Action open)
            => HandleAmbientTransactions(() =>
                {
                    open();
                    return Task.CompletedTask;
                });

        /// <summary>
        ///     Ensures the connection is enlisted in the ambient transaction if present.
        /// </summary>
        /// <param name="openAsync"> A delegate to open the underlying connection. </param>
        /// <param name="cancellationToken">
        ///     A <see cref="CancellationToken" /> to observe while waiting for the task to complete.
        /// </param>
        /// <returns> A task that represents the asynchronous operation. </returns>
        protected virtual Task HandleAmbientTransactionsAsync(
            [NotNull] Func<Task> openAsync, CancellationToken cancellationToken = default(CancellationToken))
            => HandleAmbientTransactions(openAsync);

        private Task HandleAmbientTransactions(Func<Task> open)
        {
            var current = Transaction.Current;
            if (current != null
                && !SupportsAmbientTransactions)
            {
                Dependencies.TransactionLogger.AmbientTransactionWarning(this, DateTimeOffset.UtcNow);
            }

            if (Equals(current, AmbientTransaction))
            {
                return Task.CompletedTask;
            }

            if (!_openedInternally)
            {
                // We didn't open the connection so, just try to enlist the connection in the current transaction.
                // Note that the connection can already be enlisted in a transaction (since the user opened
                // it they could enlist it manually using DbConnection.EnlistTransaction() method). If the
                // transaction the connection is enlisted in has not completed (e.g. nested transaction) this call
                // will fail (throw). Also "current" can be "null" here which means that the transaction
                // used in the previous operation has completed. In this case we should not enlist the connection
                // in "null" transaction as the user might have enlisted in a transaction manually between calls by
                // calling DbConnection.EnlistTransaction() method. Enlisting with "null" would in this case mean "unenlist"
                // and would cause an exception (see above). Had the user not enlisted in a transaction between the calls
                // enlisting with null would be a no-op - so again no reason to do it.
                if (current != null)
                {
                    DbConnection.EnlistTransaction(current);
                }
            }
            else if (_openedCount > 1)
            {
                // We opened the connection. In addition we are here because there are multiple
                // active requests going on (read: enumerators that has not been disposed yet)
                // using the same connection. (If there is only one active request e.g. like SaveChanges
                // or single enumerator there is no need for any specific transaction handling - either
                // we use the implicit ambient transaction (Transaction.Current) if one exists or we
                // will create our own local transaction. Also if there is only one active request
                // the user could not enlist it in a transaction using EntityConnection.EnlistTransaction()
                // because we opened the connection).
                // If there are multiple active requests the user might have "played" with transactions
                // after the first transaction. This code tries to deal with this kind of changes.

                if (AmbientTransaction == null)
                {
                    // Two cases here:
                    // - the previous operation was not run inside a transaction created by the user while this one is - just
                    //   enlist the connection in the transaction
                    // - the previous operation ran withing explicit transaction started with EntityConnection.EnlistTransaction()
                    //   method - try enlisting the connection in the transaction. This may fail however if the transactions
                    //   are nested as you cannot enlist the connection in the transaction until the previous transaction has
                    //   completed.
                    DbConnection.EnlistTransaction(current);
                }
                else
                {
                    // We'll close and reopen the connection to get the benefit of automatic transaction enlistment.
                    // Remarks: We get here only if there is more than one active query (e.g. nested foreach or two subsequent queries or SaveChanges
                    // inside a for each) and each of these queries are using a different transaction (note that using TransactionScopeOption.Required
                    // will not create a new transaction if an ambient transaction already exists - the ambient transaction will be used and we will
                    // not end up in this code path). If we get here we are already in a loss-loss situation - we cannot enlist to the second transaction
                    // as this would cause an exception saying that there is already an active transaction that needs to be committed or rolled back
                    // before we can enlist the connection to a new transaction. The other option (and this is what we do here) is to close and reopen
                    // the connection. This will enlist the newly opened connection to the second transaction but will also close the reader being used
                    // by the first active query. As a result when trying to continue reading results from the first query the user will get an exception
                    // saying that calling "Read" on a closed data reader is not a valid operation.
                    AmbientTransaction = current;
                    DbConnection.Close();
                    return open();
                }
            }

            AmbientTransaction = current;
            return Task.CompletedTask;
        }

        /// <summary>
        ///     Closes the connection to the database.
        /// </summary>
        /// <returns> True if the underlying connection was actually closed; false otherwise. </returns>
        public virtual bool Close()
        {
            var wasClosed = false;

            if (_openedCount > 0
                && --_openedCount == 0
                && _openedInternally)
            {
                if (DbConnection.State != ConnectionState.Closed)
                {
                    var startTime = DateTimeOffset.UtcNow;
                    var stopwatch = Stopwatch.StartNew();

                    Dependencies.ConnectionLogger.ConnectionClosing(this, startTime);

                    try
                    {
                        DbConnection.Close();

                        wasClosed = true;

                        Dependencies.ConnectionLogger.ConnectionClosed(this, startTime, stopwatch.Elapsed);
                    }
                    catch (Exception e)
                    {
                        Dependencies.ConnectionLogger.ConnectionError(
                            this,
                            e,
                            startTime,
                            stopwatch.Elapsed,
                            async: false,
                            logErrorAsDebug: false);

                        throw;
                    }
                }
                _openedInternally = false;
            }

            return wasClosed;
        }

        /// <summary>
        ///     Gets a value indicating whether the multiple active result sets feature is enabled.
        /// </summary>
        public virtual bool IsMultipleActiveResultSetsEnabled => false;

        void IResettableService.ResetState() => Dispose();

        /// <summary>
        ///     Gets a semaphore used to serialize access to this connection.
        /// </summary>
        /// <value>
        ///     The semaphore used to serialize access to this connection.
        /// </value>
        public virtual SemaphoreSlim Semaphore { get; } = new SemaphoreSlim(1);

        private readonly List<IBufferable> _activeQueries = new List<IBufferable>();
        private Transaction _enlistedTransaction;

        /// <summary>
        ///     Registers a potentially bufferable active query.
        /// </summary>
        /// <param name="bufferable"> The bufferable query. </param>
        void IRelationalConnection.RegisterBufferable(IBufferable bufferable)
        {
            Check.NotNull(bufferable, nameof(bufferable));

            if (!IsMultipleActiveResultSetsEnabled)
            {
                for (var i = _activeQueries.Count - 1; i >= 0; i--)
                {
                    _activeQueries[i].BufferAll();

                    _activeQueries.RemoveAt(i);
                }

                _activeQueries.Add(bufferable);
            }
        }

        /// <summary>
        ///     Asynchronously registers a potentially bufferable active query.
        /// </summary>
        /// <param name="bufferable"> The bufferable query. </param>
        /// <param name="cancellationToken"> The cancellation token. </param>
        /// <returns>
        ///     A Task.
        /// </returns>
        async Task IRelationalConnection.RegisterBufferableAsync(IBufferable bufferable, CancellationToken cancellationToken)
        {
            Check.NotNull(bufferable, nameof(bufferable));

            if (!IsMultipleActiveResultSetsEnabled)
            {
                for (var i = _activeQueries.Count - 1; i >= 0; i--)
                {
                    await _activeQueries[i].BufferAllAsync(cancellationToken);

                    _activeQueries.RemoveAt(i);
                }

                _activeQueries.Add(bufferable);
            }
        }

        /// <summary>
        ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public virtual void Dispose()
        {
            CurrentTransaction?.Dispose();

            if (_connectionOwned && _connection.HasValue)
            {
                DbConnection.Dispose();
                _connection.Reset(CreateDbConnection);
                _activeQueries.Clear();
                _openedCount = 0;
            }
        }
    }
}
