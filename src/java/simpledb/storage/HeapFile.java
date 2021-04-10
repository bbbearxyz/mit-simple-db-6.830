package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.beans.PersistenceDelegate;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    // File
    private File file;

    // TupleDesc
    private TupleDesc tupleDesc;

    // TableId
    private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
        tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            try {
                byte[] buffer = HeapPage.createEmptyPageData();
                raf.seek(BufferPool.getPageSize() * pid.getPageNumber());
                raf.read(buffer, 0, BufferPool.getPageSize());
                Page page = new HeapPage((HeapPageId) pid, buffer);
                if (page.getId().equals(pid)) return page;
            } finally {
                raf.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            try {
                raf.seek(BufferPool.getPageSize() * page.getId().getPageNumber());
                raf.write(page.getPageData(), 0, BufferPool.getPageSize());
            } finally {
                raf.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // System.out.println(file.length() / 4096);
        return (int)file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> list = new ArrayList<>();
        if (numPages() == 0) {
            writePage(new HeapPage(new HeapPageId(tableId, 0), HeapPage.createEmptyPageData()));
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, numPages() - 1), Permissions.READ_WRITE);
        if (page.getNumEmptySlots() == 0) {
            writePage(new HeapPage(new HeapPageId(tableId, numPages()), HeapPage.createEmptyPageData()));
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, numPages() - 1), Permissions.READ_WRITE);
        }
        page.insertTuple(t);
        list.add(page);
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            // pageNumber
            private int pageNum;

            // pageIterator
            private Iterator<Tuple> pageIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNum = 0;
                pageIterator = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pageNum), Permissions.READ_ONLY)).iterator();
            }

            private boolean advance() throws DbException, TransactionAbortedException, NoSuchElementException {
                while (true) {
                    pageNum = pageNum + 1;
                    if (pageNum >= numPages()) return false;
                    //System.out.println("numPages: " + numPages());
                    //System.out.println("pageNum: " + pageNum);
                    pageIterator = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pageNum), Permissions.READ_ONLY)).iterator();
                    if (pageIterator.hasNext()) return true;
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (pageIterator == null) return false;
                if (pageIterator.hasNext()) return true;
                else return advance();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (pageIterator == null) throw new NoSuchElementException();
                hasNext();
                return pageIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                pageIterator = null;
            }
        };
    }

}

