package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    // aggregator
    Aggregator aggregator;

    // child
    OpIterator[] children = new OpIterator[1];

    // aField
    int aField;

    // gField
    int gField;

    // Op
    Aggregator.Op op;

    // Iterator
    OpIterator opIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.children[0] = child;
        aField = afield;
        gField = gfield;
        op = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return children[0].getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return children[0].getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    private void init() {
        if (children[0].getTupleDesc().getFieldType(aField) == Type.INT_TYPE) {
            if (gField != Aggregator.NO_GROUPING) {
                aggregator = new IntegerAggregator(gField, children[0].getTupleDesc().getFieldType(gField), aField, op);
            } else {
                aggregator = new IntegerAggregator(gField, null, aField, op);
            }
        } else {
            if (gField != Aggregator.NO_GROUPING) {
                aggregator = new StringAggregator(gField, children[0].getTupleDesc().getFieldType(gField), aField, op);
            } else {
                aggregator = new IntegerAggregator(gField, null, aField, op);
            }
        }
    }
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        init();
        children[0].open();
        while (children[0].hasNext()) {
            Tuple tuple = children[0].next();
            // System.out.println("add" + tuple + ".....");
            aggregator.mergeTupleIntoGroup(tuple);
        }
        children[0].close();
        if (opIterator == null) {
            opIterator = aggregator.iterator();
            opIterator.open();
        } else {
            opIterator.rewind();
        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (opIterator == null) throw new DbException("not open yet");
        if (opIterator.hasNext()) {
            Tuple tuple = opIterator.next();
            System.out.println("......." + tuple);
            return tuple;
        }
        else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        init();
        return aggregator.iterator().getTupleDesc();
    }

    public void close() {
        // some code goes here
        super.close();
        opIterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children[0] = children[0];
    }

}
