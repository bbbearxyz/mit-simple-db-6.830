package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

import simpledb.storage.Field;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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

    // map gp to count
    // used for avg operation
    HashMap<Field, Integer> hashCount;

    // sum
    int sum;

    // count
    int count;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.op = what;
        if (gbfield != NO_GROUPING) {
            hash = new HashMap<>();
            hashCount = new HashMap<>();
        } else {
            sum = 0;
            count = 0;
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbField != NO_GROUPING) {
            Field field = tup.getField(gbField);
            if (hash.containsKey(tup.getField(gbField))) {
                if (op == Op.AVG) {
                    hash.put(field, hash.get(field) + ((IntField)tup.getField(afield)).getValue());
                    hashCount.put(field, hashCount.get(field) + 1);
                } else if (op == Op.COUNT) {
                    hash.put(field, hash.get(field) + 1);
                } else if (op == Op.SUM) {
                    hash.put(field, hash.get(field) + ((IntField)tup.getField(afield)).getValue());
                    // System.out.println(hash.get(field));
                } else if (op == Op.MAX) {
                    hash.put(field, Math.max(hash.get(field), ((IntField)tup.getField(afield)).getValue()));
                } else if (op == Op.MIN) {
                    hash.put(field, Math.min(hash.get(field), ((IntField)tup.getField(afield)).getValue()));
                }
            } else {
                if (op == Op.AVG) {
                    hash.put(field, ((IntField)tup.getField(afield)).getValue());
                    hashCount.put(field, 1);
                } else if (op == Op.COUNT) {
                    hash.put(field, 1);
                } else if (op == Op.SUM) {
                    hash.put(field, ((IntField)tup.getField(afield)).getValue());
                } else if (op == Op.MAX) {
                    hash.put(field, ((IntField)tup.getField(afield)).getValue());
                } else if (op == Op.MIN) {
                    hash.put(field, ((IntField) tup.getField(afield)).getValue());
                }
            }
        } else {
            if (op == Op.AVG) {
                count ++;
                sum += ((IntField)tup.getField(afield)).getValue();
            } else if (op == Op.COUNT) {
                sum ++;
                count ++;
            } else if (op == Op.SUM) {
                sum += ((IntField)tup.getField(afield)).getValue();
                count ++;
            } else if (op == Op.MAX) {
                if (count == 0) sum = ((IntField)tup.getField(afield)).getValue();
                sum = Integer.max(((IntField)tup.getField(afield)).getValue(), sum);
            } else if (op == Op.MIN) {
                if (count == 0) sum = ((IntField)tup.getField(afield)).getValue();
                sum = Integer.min(((IntField)tup.getField(afield)).getValue(), sum);
            }
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {

            Iterator<Map.Entry<Field, Integer>> iterator;

            TupleDesc td;

            Tuple tuple;

            private Boolean hasNext;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (gbField != NO_GROUPING) {
                    iterator = hash.entrySet().iterator();
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
                    hasNext = true;
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (gbField != NO_GROUPING) {
                    if (iterator == null) throw new DbException("not yet open");
                    if (iterator.hasNext()) return true;
                    else return false;
                } else {
                    if (td == null) throw new DbException("not yet open");
                    return hasNext;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (gbField != NO_GROUPING) {
                    if (iterator == null) throw new NoSuchElementException();
                    Map.Entry<Field, Integer> next = iterator.next();
                    tuple.setField(0, next.getKey());
                    if (op == Op.AVG) {
                        Field field = new IntField(next.getValue() / hashCount.get(next.getKey()));
                        tuple.setField(1, field);
                    } else {
                        Field field = new IntField(next.getValue());
                        // System.out.println(next.getValue());
                        tuple.setField(1, field);
                    }
                } else {
                    if (td == null) throw new NoSuchElementException();
                    if (op == Op.AVG) {
                        Field field = new IntField(sum / count);
                        tuple.setField(0, field);
                    } else {
                        Field field = new IntField(sum);
                        tuple.setField(0, field);
                    }
                    hasNext = false;
                    //System.out.println("....." + field);
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
