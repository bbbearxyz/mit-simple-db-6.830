package simpledb.execution;

import com.sun.org.apache.xpath.internal.operations.Bool;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    // tid
    TransactionId tid;

    // OpIterator
    OpIterator child;

    Boolean first;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.first = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] type = new Type[1];
        type[0] = Type.INT_TYPE;
        TupleDesc td = new TupleDesc(type);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        first = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        Type[] type = new Type[1];
        type[0] = Type.INT_TYPE;
        TupleDesc td = new TupleDesc(type);
        Tuple tuple = new Tuple(td);
        int count = 0;
        try {
            while (child.hasNext()) {
                Tuple t = child.next();
                Database.getBufferPool().deleteTuple(tid, t);
                count ++;
            }
        } catch (IOException e) {
            throw new DbException("error in fetchNext");
        }
        tuple.setField(0, new IntField(count));
        if (first) return null;
        first = true;
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] children = new OpIterator[1];
        children[0] = this.child;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
