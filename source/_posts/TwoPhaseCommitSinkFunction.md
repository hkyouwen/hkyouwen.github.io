```
@PublicEvolving
public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT>
		extends RichSinkFunction<IN>
		implements CheckpointedFunction, CheckpointListener {
```

​    TwoPhaseCommitSinkFunction是flink用来实现exactly-once语义的sink，继承了产生检查点的CheckpointedFunction以及检查点通知的CheckpointListener。

主要函数：

```
@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

		state = context.getOperatorStateStore().getListState(stateDescriptor);

		if (context.isRestored()) {
			LOG.info("{} - restoring state", name());

			for (State<TXN, CONTEXT> operatorState : state.get()) {
				userContext = operatorState.getContext();
				List<TransactionHolder<TXN>> recoveredTransactions = operatorState.getPendingCommitTransactions();
				for (TransactionHolder<TXN> recoveredTransaction : recoveredTransactions) {
					// If this fails to succeed eventually, there is actually data loss
					recoverAndCommitInternal(recoveredTransaction);
					LOG.info("{} committed recovered transaction {}", name(), recoveredTransaction);
				}

				recoverAndAbort(operatorState.getPendingTransaction().handle);
				LOG.info("{} aborted recovered transaction {}", name(), operatorState.getPendingTransaction());

				if (userContext.isPresent()) {
					finishRecoveringContext();
				}
			}
		}
		// if in restore we didn't get any userContext or we are initializing from scratch
		if (userContext == null) {
			LOG.info("{} - no state to restore", name());

			userContext = initializeUserContext();
		}
		this.pendingCommitTransactions.clear();

		currentTransactionHolder = beginTransactionInternal();
		LOG.debug("{} - started new transaction '{}'", name(), currentTransactionHolder);
	}
```

​    启动时恢复至上一个检查点的状态，recoverAndCommitInternal重新提交pre-commit的事务，**这里提交失败会重启任务，最终失败会造成数据丢失**。recoverAndAbort重新删除已经提交但因为中断未删除的事务。并通过beginTransactionInternal开启新一轮事务。

```
private void recoverAndCommitInternal(TransactionHolder<TXN> transactionHolder) {
		try {
			logWarningIfTimeoutAlmostReached(transactionHolder);
			recoverAndCommit(transactionHolder.handle);
		} catch (final Exception e) {
			final long elapsedTime = clock.millis() - transactionHolder.transactionStartTime;
			if (ignoreFailuresAfterTransactionTimeout && elapsedTime > transactionTimeout) {
				LOG.error("Error while committing transaction {}. " +
						"Transaction has been open for longer than the transaction timeout ({})." +
						"Commit will not be attempted again. Data loss might have occurred.",
					transactionHolder.handle, transactionTimeout, e);
			} else {
				throw e;
			}
		}
	}
```

​    使用recoverAndCommit再次提交，提交异常时，判断事务是否超时，若超时则此次提交失败，否则抛出异常。

```
protected void recoverAndCommit(TXN transaction) {
		commit(transaction);
	}
	
protected void recoverAndAbort(TXN transaction) {
		abort(transaction);
	}
	
private TransactionHolder<TXN> beginTransactionInternal() throws Exception {
		return new TransactionHolder<>(beginTransaction(), clock.millis());
	}
```

```
@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// this is like the pre-commit of a 2-phase-commit transaction
		// we are ready to commit and remember the transaction

		checkState(currentTransactionHolder != null, "bug: no transaction object when performing state snapshot");

		long checkpointId = context.getCheckpointId();
		LOG.debug("{} - checkpoint {} triggered, flushing transaction '{}'", name(), context.getCheckpointId(), currentTransactionHolder);

		preCommit(currentTransactionHolder.handle);
		pendingCommitTransactions.put(checkpointId, currentTransactionHolder);
		LOG.debug("{} - stored pending transactions {}", name(), pendingCommitTransactions);

		currentTransactionHolder = beginTransactionInternal();
		LOG.debug("{} - started new transaction '{}'", name(), currentTransactionHolder);

		state.clear();
		state.add(new State<>(
			this.currentTransactionHolder,
			new ArrayList<>(pendingCommitTransactions.values()),
			userContext));
	}
```

​    snapshotState函数实现自CheckpointedFunction，用于产生checkpoint，这一步生成barrier，并调用preCommit产生一个预提交的事务，存入pendingCommitTransactions并终止当前数据处理。 调用beginTransactionInternal重新开启新一轮事务并继续下一轮处理。checkpoint更新state用于中断后恢复数据。

```
@Override
	public final void notifyCheckpointComplete(long checkpointId) throws Exception {
		// the following scenarios are possible here
		//
		//  (1) there is exactly one transaction from the latest checkpoint that
		//      was triggered and completed. That should be the common case.
		//      Simply commit that transaction in that case.
		//
		//  (2) there are multiple pending transactions because one previous
		//      checkpoint was skipped. That is a rare case, but can happen
		//      for example when:
		//
		//        - the master cannot persist the metadata of the last
		//          checkpoint (temporary outage in the storage system) but
		//          could persist a successive checkpoint (the one notified here)
		//
		//        - other tasks could not persist their status during
		//          the previous checkpoint, but did not trigger a failure because they
		//          could hold onto their state and could successfully persist it in
		//          a successive checkpoint (the one notified here)
		//
		//      In both cases, the prior checkpoint never reach a committed state, but
		//      this checkpoint is always expected to subsume the prior one and cover all
		//      changes since the last successful one. As a consequence, we need to commit
		//      all pending transactions.
		//
		//  (3) Multiple transactions are pending, but the checkpoint complete notification
		//      relates not to the latest. That is possible, because notification messages
		//      can be delayed (in an extreme case till arrive after a succeeding checkpoint
		//      was triggered) and because there can be concurrent overlapping checkpoints
		//      (a new one is started before the previous fully finished).
		//
		// ==> There should never be a case where we have no pending transaction here
		//

		Iterator<Map.Entry<Long, TransactionHolder<TXN>>> pendingTransactionIterator = pendingCommitTransactions.entrySet().iterator();
		checkState(pendingTransactionIterator.hasNext(), "checkpoint completed, but no transaction pending");

		while (pendingTransactionIterator.hasNext()) {
			Map.Entry<Long, TransactionHolder<TXN>> entry = pendingTransactionIterator.next();
			Long pendingTransactionCheckpointId = entry.getKey();
			TransactionHolder<TXN> pendingTransaction = entry.getValue();
			if (pendingTransactionCheckpointId > checkpointId) {
				continue;
			}

			LOG.info("{} - checkpoint {} complete, committing transaction {} from checkpoint {}",
				name(), checkpointId, pendingTransaction, pendingTransactionCheckpointId);

			logWarningIfTimeoutAlmostReached(pendingTransaction);
			commit(pendingTransaction.handle);

			LOG.debug("{} - committed checkpoint transaction {}", name(), pendingTransaction);

			pendingTransactionIterator.remove();
		}
	}

```

依此提交所有当前checkpointId之前的pre-commit事务

```
@Internal
	public static final class TransactionHolder<TXN> {

		private final TXN handle;

		/**
		 * The system time when {@link #handle} was created.
		 * Used to determine if the current transaction has exceeded its timeout specified by
		 * {@link #transactionTimeout}.
		 */
		private final long transactionStartTime;

		@VisibleForTesting
		public TransactionHolder(TXN handle, long transactionStartTime) {
			this.handle = handle;
			this.transactionStartTime = transactionStartTime;
		}

		long elapsedTime(Clock clock) {
			return clock.millis() - transactionStartTime;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TransactionHolder<?> that = (TransactionHolder<?>) o;

			if (transactionStartTime != that.transactionStartTime) {
				return false;
			}
			return handle != null ? handle.equals(that.handle) : that.handle == null;
		}

		@Override
		public int hashCode() {
			int result = handle != null ? handle.hashCode() : 0;
			result = 31 * result + (int) (transactionStartTime ^ (transactionStartTime >>> 32));
			return result;
		}

		@Override
		public String toString() {
			return "TransactionHolder{" +
				"handle=" + handle +
				", transactionStartTime=" + transactionStartTime +
				'}';
		}
	}
```

TransactionHolder，存放事务及事务创建时间

以下为用户自定义函数：

```
/**
* Write value within a transaction.
*/
protected abstract void invoke(TXN transaction, IN value, Context context) throws Exception;

/**
* Method that starts a new transaction.
*
* @return newly created transaction.
*/
protected abstract TXN beginTransaction() throws Exception;

/**
* Pre commit previously created transaction. Pre commit must make all of the necessary steps to prepare the
* transaction for a commit that might happen in the future. After this point the transaction might still be
* aborted, but underlying implementation must ensure that commit calls on already pre committed transactions
* will always succeed.
*
* <p>Usually implementation involves flushing the data.
*/
protected abstract void preCommit(TXN transaction) throws Exception;

/**
* Commit a pre-committed transaction. If this method fail, Flink application will be
* restarted and {@link TwoPhaseCommitSinkFunction#recoverAndCommit(Object)} will be called again for the
* same transaction.
*/
protected abstract void commit(TXN transaction);

/**
* Abort a transaction.
*/
protected abstract void abort(TXN transaction);
```

