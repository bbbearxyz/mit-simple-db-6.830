package simpledb.storage;

import simpledb.optimizer.TableStats;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// 未处理写锁进读锁的情形

public class LockManager {

    // in the hashmap, tid -> ArrayList<Lock>
    private ConcurrentHashMap<TransactionId, ArrayList<ReentrantReadWriteLock>> tidLockMap;

    private ConcurrentHashMap<TransactionId, ArrayList<Boolean>> tidTypeMap;

    // in the hashmap, pid -> lock
    private ConcurrentHashMap<PageId, ReentrantReadWriteLock> pidLockMap;

    private ConcurrentHashMap<PageId, Integer> pidCount;

    private ConcurrentHashMap<TransactionId, ArrayList<PageId>> tidPidMap;

    // Lock
    private  Lock startLock = new ReentrantLock();

    public LockManager() {
        tidLockMap = new ConcurrentHashMap<>();
        pidLockMap = new ConcurrentHashMap<>();
        tidTypeMap = new ConcurrentHashMap<>();
        pidCount = new ConcurrentHashMap<>();
        tidPidMap = new ConcurrentHashMap<>();
    }

    public  void getWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException, InterruptedException {
        startLock.lock();
        if (!pidLockMap.containsKey(pid)) {
            pidLockMap.put(pid, new ReentrantReadWriteLock());
        }
        if (!tidLockMap.containsKey(tid)) {
            tidLockMap.put(tid, new ArrayList<>());
        }
        if (!tidPidMap.containsKey(tid)) {
            tidPidMap.put(tid, new ArrayList<>());
        }
        if (!tidTypeMap.containsKey(tid)) {
            tidTypeMap.put(tid, new ArrayList<>());
        }
        if (!pidCount.containsKey(pid)) {
            pidCount.put(pid, 0);
        }
        startLock.unlock();
        if (check(tid, pid)) {
            ArrayList<PageId> pageIds = tidPidMap.get(tid);
            for (int i = 0; i < pageIds.size(); i ++ ) {
                if (pageIds.get(i).equals(pid) && !tidTypeMap.get(tid).get(i)) {
                    tidLockMap.get(tid).get(i).readLock().unlock();
                    if (!pidLockMap.get(pid).writeLock().tryLock(1, TimeUnit.SECONDS)) {
                        throw new TransactionAbortedException();
                    }
                    tidTypeMap.get(tid).set(i, true);
                }
            }
            return;
        }
        if (!pidLockMap.get(pid).writeLock().tryLock(1, TimeUnit.SECONDS)) {
            throw new TransactionAbortedException();
        }
        pidCount.put(pid, pidCount.get(pid) + 1);
        tidLockMap.get(tid).add(pidLockMap.get(pid));
        tidTypeMap.get(tid).add(true);
        tidPidMap.get(tid).add(pid);
        System.out.println(tid.getId() + "get write lock on " + pid.getPageNumber());
    }

    public void getReadLock(TransactionId tid, PageId pid) throws InterruptedException, TransactionAbortedException {
        startLock.lock();
        if (!pidLockMap.containsKey(pid)) {
            pidLockMap.put(pid, new ReentrantReadWriteLock());
        }
        if (!tidLockMap.containsKey(tid)) {
            tidLockMap.put(tid, new ArrayList<>());
        }
        if (!tidPidMap.containsKey(tid)) {
            tidPidMap.put(tid, new ArrayList<>());
        }
        if (!tidTypeMap.containsKey(tid)) {
            tidTypeMap.put(tid, new ArrayList<>());
        }
        if (!pidCount.containsKey(pid)) {
            pidCount.put(pid, 0);
        }
        startLock.unlock();
        if (check(tid, pid)) {
            return;
        }
        if (!pidLockMap.get(pid).readLock().tryLock(1, TimeUnit.SECONDS)) {
            throw new TransactionAbortedException();
        }
        pidCount.put(pid, pidCount.get(pid) + 1);
        tidLockMap.get(tid).add(pidLockMap.get(pid));
        tidTypeMap.get(tid).add(false);
        tidPidMap.get(tid).add(pid);
        System.out.println(tid.getId() + "get read lock on " + pid.getPageNumber());
    }

    public boolean release(TransactionId tid) {
        for (int i = 0; i < tidPidMap.get(tid).size(); i ++ ) {
            if (tidTypeMap.get(tid).get(i)) {
                tidLockMap.get(tid).get(i).writeLock().unlock();
            } else {
                tidLockMap.get(tid).get(i).readLock().unlock();
            }
            PageId pageId = tidPidMap.get(tid).get(i);
            pidCount.put(pageId, pidCount.get(pageId) - 1);
        }
        tidLockMap.remove(tid);
        tidPidMap.remove(tid);
        tidTypeMap.remove(tid);
        return true;
    }

    public boolean release(TransactionId tid, PageId pid) {
        for (int i = 0; i < tidPidMap.get(tid).size(); i ++ ) {
            if (tidPidMap.get(tid).get(i).equals(pid)) {
                PageId pageId = tidPidMap.get(tid).get(i);
                tidPidMap.get(tid).remove(i);
                if (tidTypeMap.get(tid).get(i)) {
                    tidLockMap.get(tid).get(i).writeLock().unlock();
                } else {
                    tidLockMap.get(tid).get(i).readLock().unlock();
                }
                pidCount.put(pageId, pidCount.get(pageId) - 1);
                tidLockMap.get(tid).remove(i);
                tidTypeMap.get(tid).remove(i);
                return true;
            }
        }
        return false;
    }

    public boolean check(TransactionId tid, PageId pid) {
        ArrayList<PageId> pageIds = tidPidMap.get(tid);
        for (PageId pageId : pageIds) {
            if (pageId.equals(pid)) return true;
        }
        return false;
    }

    public boolean isLocked(PageId pid) {
        if (pidCount.get(pid) == 0) return false;
        return true;
    }

    public ArrayList<PageId> getPages(TransactionId tid) {
        return tidPidMap.get(tid);
    }


}
