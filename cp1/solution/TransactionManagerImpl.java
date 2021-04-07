package cp1.solution;

import cp1.base.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

/* I'm sorry there's so few comments but I started working on the problem
   too late and didn't have time to add more comments. However, the code uses synchronized(object)
   a lot so it should be fairly easy to follow as compared to a code with lots of mutexes.
 */

public class TransactionManagerImpl implements TransactionManager {

    /* A class that contains data about a single transaction */
    private static class Transaction {
        final HashSet<ResourceId> ownedResources;
        private final long creationTime;
        private final Thread ownerThread;
        private boolean isAborted;
        private final Deque<Pair<Resource, ResourceOperation>> appliedOperations;

        public Transaction(long time, Thread threadId) {
            isAborted = false;
            ownedResources = new HashSet<>();
            creationTime = time;
            ownerThread = threadId;
            appliedOperations = new ArrayDeque<>();
        }

        public boolean isAborted() {
            return this.isAborted;
        }

        public void markAborted() {
            isAborted = true;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public Thread getOwnerThread() {
            return ownerThread;
        }

        public HashSet<ResourceId> getOwnedResources() {
            return ownedResources;
        }

        public void addOwnedResource(ResourceId rid) {
            ownedResources.add(rid);
        }

        public boolean hasOperationsOnStack() {
            return !appliedOperations.isEmpty();
        }

        public Pair<Resource, ResourceOperation> popOperationFromStack() {
            return appliedOperations.pop();
        }

        public void pushOperationOnStack(Pair<Resource, ResourceOperation> operationPair) {
            appliedOperations.push(operationPair);
        }
    }

    final ConcurrentHashMap<Thread, Transaction> transactions;
    final HashMap<ResourceId, Transaction> resourceOwnership;
    final LocalTimeProvider timeProvider;
    final Collection<Resource> resources;
    final HashMap<ResourceId, List<Transaction>> waitingForResource;

    public TransactionManagerImpl(Collection<Resource> resources, LocalTimeProvider timeProvider) {
        this.transactions = new ConcurrentHashMap<>();
        this.timeProvider = timeProvider;
        this.resources = resources;
        this.resourceOwnership = new HashMap<>();
        this.waitingForResource = new HashMap<>();

        for (Resource resource : resources) {
            resourceOwnership.put(resource.getId(), null);
            waitingForResource.put(resource.getId(), new ArrayList<>());
        }
    }

    @Override
    synchronized public void startTransaction() throws AnotherTransactionActiveException {
        if (transactions.containsKey(Thread.currentThread())) {
            throw new AnotherTransactionActiveException();
        }

        var transaction = new Transaction(timeProvider.getTime(), Thread.currentThread());
        transactions.put(Thread.currentThread(), transaction);
    }

    private ArrayList<Transaction> recursivelyLookForDeadlockCycle(Transaction suspiciousTransaction,
                                                                   HashSet<ResourceId> cantRequestThese,
                                                                   int max_depth) {
        if (max_depth == 0) {
            return null;
        }

        // Is waiting for current transaction?
        for (var rid : cantRequestThese) {
            if (waitingForResource.get(rid).contains(suspiciousTransaction)) {
                var list = new ArrayList<Transaction>();
                list.add(suspiciousTransaction);
                return list;
            }
        }

        // Is waiting for a transaction that is waiting for current transaction?
        for (var rid : waitingForResource.keySet()) {
            if (waitingForResource.get(rid).contains(suspiciousTransaction)) {
                var resourceOwner = resourceOwnership.get(rid);
                var furtherInCycle = recursivelyLookForDeadlockCycle(resourceOwner, cantRequestThese, max_depth - 1);
                if (furtherInCycle != null) {
                    furtherInCycle.add(suspiciousTransaction);
                    return furtherInCycle;
                }
                // A transaction can only be awaiting 1 resource at a time so no need to look further.
                break;
            }
        }

        return null;
    }

    private ArrayList<Transaction> findDeadlockCycleInCurrentTransaction(ResourceId lastWantedResource) {
        var cantRequestThese = transactions.get(Thread.currentThread()).getOwnedResources();
        var wantedResourceOwner = resourceOwnership.get(lastWantedResource);
        var cycle = recursivelyLookForDeadlockCycle(wantedResourceOwner, cantRequestThese, transactions.size() * 2 + 1);
        if (cycle != null) {
            cycle.add(transactions.get(Thread.currentThread()));
        } else {
            cycle = new ArrayList<>();
        }
        return cycle;
    }

    /* Returns the newest transaction in the collection */
    private Transaction pickTransactionToAbort(ArrayList<Transaction> candidates) {
        long maxCreationTime = candidates.get(0).getCreationTime();
        for (var t : candidates) {
            if (t.getCreationTime() > maxCreationTime) {
                maxCreationTime = t.getCreationTime();
            }
        }

        long finalMaxCreationTime = maxCreationTime;
        Supplier<Stream<Transaction>>
                withMinTimeStream = () -> candidates.stream().filter(t -> t.getCreationTime() == finalMaxCreationTime);
        if (withMinTimeStream.get().count() == 1) {
            return withMinTimeStream.get().findFirst().get();
        }
        return withMinTimeStream.get().max((t1, t2) -> Long.compare(t1.getOwnerThread().getId(),
                t2.getOwnerThread().getId())).get();
    }

    private void acquireResourceOwnership(ResourceId rid) throws ActiveTransactionAborted, InterruptedException {
        var transaction = transactions.get(Thread.currentThread());

        synchronized (waitingForResource) {
            waitingForResource.get(rid).add(transaction);
        }

        synchronized (resourceOwnership) {
            while (resourceOwnership.get(rid) != null) {
                var deadlockCycle = findDeadlockCycleInCurrentTransaction(rid);

                if (!deadlockCycle.isEmpty()) {
                    var toAbort = pickTransactionToAbort(deadlockCycle);
                    synchronized (transactions) {
                        toAbort.markAborted();
                        toAbort.getOwnerThread().interrupt();
                    }
                }

                try {
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    resourceOwnership.wait();
                } catch (InterruptedException e) {
                    synchronized (waitingForResource) {
                        waitingForResource.get(rid).remove(transaction);
                    }
                    synchronized (transactions.get(Thread.currentThread())) {
                        if (transaction.isAborted()) {
                            throw new ActiveTransactionAborted();
                        }
                    }
                    throw e;
                }
            }

            resourceOwnership.put(rid, transaction);
            transaction.addOwnedResource(rid);
            synchronized (waitingForResource) {
                waitingForResource.get(rid).remove(transaction);
            }

            resourceOwnership.notify();
        }
    }

    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation)
            throws NoActiveTransactionException, UnknownResourceIdException, ActiveTransactionAborted,
            ResourceOperationException, InterruptedException {
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        }
        if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }

        var maybe_resource = resources.stream().filter(res -> res.getId().compareTo(rid) == 0).findFirst();
        if (maybe_resource.isEmpty()) {
            throw new UnknownResourceIdException(rid);
        }
        var resource = maybe_resource.get();

        var transaction = transactions.get(Thread.currentThread());
        synchronized (resourceOwnership) {
            if (!(resourceOwnership.get(rid) != null && resourceOwnership.get(rid).equals(transaction))) {
                acquireResourceOwnership(rid);
            }
        }

        operation.execute(resource);
        if (Thread.interrupted()) {
            operation.undo(resource);
            throw new InterruptedException();
        } else {
            synchronized (transactions) {
                transaction.pushOperationOnStack(new Pair<>(resource, operation));
            }
        }
    }

    /* Remove information about resources acquired by the current transaction */
    private void freeCurrentTransactionsResources() {
        synchronized (transactions.get(Thread.currentThread())) {
            var transaction = transactions.get(Thread.currentThread());
            synchronized (resourceOwnership) {
                for (var resourceId : transaction.getOwnedResources()) {
                    resourceOwnership.put(resourceId, null);
                }
            }
            transaction.getOwnedResources().clear();
            synchronized (resourceOwnership) {
                // Needed to notify transactions waiting to acquire resources.
                resourceOwnership.notifyAll();
            }
        }
    }

    @Override
    public void commitCurrentTransaction() throws NoActiveTransactionException, ActiveTransactionAborted {
        if (!transactions.containsKey(Thread.currentThread())) {
            throw new NoActiveTransactionException();
        }
        if (transactions.get(Thread.currentThread()).isAborted()) {
            throw new ActiveTransactionAborted();
        }

        freeCurrentTransactionsResources();

        synchronized (transactions) {
            transactions.remove(Thread.currentThread());
        }
    }

    @Override
    public void rollbackCurrentTransaction() {
        if (!transactions.containsKey(Thread.currentThread())) {
            return;
        }
        var transaction = transactions.get(Thread.currentThread());
        while (transaction.hasOperationsOnStack()) {
            var operationResourcePair = transaction.popOperationFromStack();
            operationResourcePair.second().undo(operationResourcePair.first());
        }

        freeCurrentTransactionsResources();

        synchronized (transactions) {
            transactions.remove(Thread.currentThread());
        }
    }

    @Override
    public boolean isTransactionActive() {
        return transactions.containsKey(Thread.currentThread());
    }

    @Override
    public boolean isTransactionAborted() {
        if (transactions.containsKey(Thread.currentThread())) {
            synchronized (transactions.get(Thread.currentThread())) {
                return transactions.get(Thread.currentThread()).isAborted();
            }
        }
        return false;
    }
}
