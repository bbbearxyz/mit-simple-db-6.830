package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.execution.SeqScan;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    // PageArr
    private ArrayList<Page> pageArr;

    private ReentrantLock lock;

    // PageMap from integer to Page
    private HashMap<PageId, Page> pageMap;

    // Maximum size
    private int maxSize;

    private LockManager lockManager;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pageArr = new ArrayList<>();
        pageMap = new HashMap<>();
        maxSize = numPages;
        lockManager = new LockManager();
        lock = new ReentrantLock();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    Page get(PageId pid) {
        lock.lock();
        if (pageMap.containsKey(pid)) {
            Page page = pageMap.get(pid);
            int index = pageArr.indexOf(page);
            pageArr.remove(index);
            pageArr.add(page);
            pageMap.put(pid, page);
            lock.unlock();
            return page;
        }
        lock.unlock();
        return null;
    }

    void put(Page page, TransactionId tid) throws IOException, DbException {
        lock.lock();
        if (pageMap.containsKey(page.getId())) {
            PageId pid = page.getId();
            pageArr.remove(pageArr.indexOf(page));
            pageArr.add(page);
            pageMap.put(pid, page);
        } else {
            if (pageArr.size() < maxSize) {
                pageArr.add(page);
                pageMap.put(page.getId(), page);
            } else {
                evictPage();
                pageArr.add(page);
                pageMap.put(page.getId(), page);
            }
        }
        lock.unlock();
    }
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());

        // apply for lock
        try {
            if (perm == Permissions.READ_ONLY) {
                lockManager.getReadLock(tid, pid);
            } else {
                lockManager.getWriteLock(tid, pid);
            }
        } catch (InterruptedException e) {
            System.out.println("interruption.");
        }

        Page page = get(pid);
        if (page != null) return page;

        page = file.readPage(pid);
        try {
            put(page, tid);
        } catch(Exception e) {
            e.printStackTrace();
            throw new DbException("a put exception in the bufferPool");
        }
        return page;
    }

    public void print(TransactionId tid, int tableId) throws DbException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        DbFileIterator iterator = file.iterator(tid);
        iterator.open();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.check(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (Exception e) {
                System.out.println("error in the flushing pages between transaction commit ");
            }
        } else {
            ArrayList<PageId> pages = lockManager.getPages(tid);
            for (PageId pid : pages) {
                if (pageMap.get(pid).isDirty() != null) {
                    discardPage(pid);
                }
            }
        }
        lockManager.release(tid);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            put(page, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> pages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            put(page, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Map.Entry<PageId, Page>> iterator = pageMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> next = iterator.next();
            if (next.getValue().isDirty() != null) {
                flushPage(next.getKey());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        lock.lock();
        pageArr.remove(pageMap.get(pid));
        pageMap.remove(pid);
        lock.unlock();
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(pageMap.get(pid));
        if (pageMap.get(pid).isDirty() == null) {
           throw new IOException("the page is not dirty");
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        ArrayList<PageId> pages = lockManager.getPages(tid);
        for (PageId pid : pages) {
            if (pageMap.get(pid).isDirty().equals(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if (pageArr.size() == 0) throw new DbException("no page in the bufferPool");
        for (int i = 0; i < pageArr.size(); i ++ ) {
            if (pageArr.get(i).isDirty() == null && !lockManager.isLocked(pageArr.get(i).getId())) {
                discardPage(pageArr.get(0).getId());
                break;
            }
        }
    }

}
