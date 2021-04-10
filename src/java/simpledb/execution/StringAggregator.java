package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // gbField
    int gbField;

    // gbFieldType
    Type gbFieldType;

    // afield
    int afield;

    // Op
    Op op;

    // map gb to value
    HashMap<Field, Integer> hash;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.op = what;
        hash = new HashMap<>();
        if (op != Op.COUNT) throw new IllegalArgumentException();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = tup.getField(gbField);
        if (hash.containsKey(field)) {
            hash.put(field, hash.get(field) + 1);
        } else {
            hash.put(null, hash.get(null) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {

            Iterator<Map.Entry<Field, Integer>> iterator;

            TupleDesc td;

            Tuple tuple;


            @Override
            public void open() throws DbException, TransactionAbortedException {
                iterator = hash.entrySet().iterator();
                if (gbField != NO_GROUPING) {
                    if (td == null) {
                        Type[] typeArr = new Type[2];
                        typeArr[0] = gbFieldType;
                        typeArr[1] = Type.INT_TYPE;
                        td = new TupleDesc(typeArr);
                        tuple = new Tuple(td);
                    }
                } else {
                    if (td == null) {
                        Type[] typeArr = new Type[1];
                        typeArr[0] = Type.INT_TYPE;
                        td = new TupleDesc(typeArr);
                        tuple = new Tuple(td);
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iterator == null) throw new DbException("not yet open");
                if (iterator.hasNext()) return true;
                else return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (iterator == null) throw new NoSuchElementException();
                Map.Entry<Field, Integer> next = iterator.next();
                if (gbField != NO_GROUPING) {
                    tuple.setField(0, next.getKey());
                    Field field = new IntField(next.getValue());
                    tuple.setField(1, field);
                } else {
                    Field field = new IntField(next.getValue());
                    tuple.setField(0, field);
                }
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                if (gbField != NO_GROUPING) {
                    if (td == null) {
                        Type[] typeArr = new Type[2];
                        typeArr[0] = gbFieldType;
                        typeArr[1] = Type.INT_TYPE;
                        return new TupleDesc(typeArr);
                    }
                } else {
                    if (td == null) {
                        Type[] typeArr = new Type[1];
                        typeArr[0] = Type.INT_TYPE;
                        return new TupleDesc(typeArr);
                    }
                }
                return null;
            }

            @Override
            public void close() {
                iterator = null;
                td = null;
                tuple = null;
            }
        };
    }

}
