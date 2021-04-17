package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.awt.*;
import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    // intArray
    int[] intArr;

    // min
    int min;

    // max
    int max;

    // all
    int all;

    // width
    int width;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        intArr = new int[buckets];
        this.min = min;
        this.max = max;
        this.all = 0;
        if ((max - min + 1) % buckets == 0) {
            this.width = Integer.max(1, (max - min + 1) / buckets);
        } else {
            this.width = (max - min + 1) / buckets + 1;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        intArr[(v - min) / width] ++;
        all ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double res = 0;
        if (op == Predicate.Op.EQUALS) {
            if (v < min || v > max) return 0;
            res = (double) (intArr[(v - min) / width]) / (width * all);
        } else if (op == Predicate.Op.NOT_EQUALS) {
            if (v < min || v > max) return 1;
            res = 1 - (double) (intArr[(v - min) / width]) / (width * all);
        } else if (op == Predicate.Op.GREATER_THAN) {
            if (v > max) return 0;
            if (v < min) return 1;
            int items = 0;
            for (int i = (v - min) / width + 1; i < intArr.length; i ++ ) {
                items += intArr[i];
            }
            res = (double) items / all;
            res += (double) (intArr[(v - min) / width]) / (width * all) * ((v - min) / width * width + width - v);
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (v > max) return 0;
            if (v < min) return 1;
            int items = 0;
            for (int i = (v - min) / width + 1; i < intArr.length; i ++ ) {
                items += intArr[i];
            }
            res = (double) items / all;
            res += (double) (intArr[(v - min) / width]) / (width * all) * ((v - min) / width * width + width - v) + (double) (intArr[(v - min) / width]) / (width * all);
        } else if (op == Predicate.Op.LESS_THAN) {
            if (v < min) return 0;
            if (v > max) return 1;
            int items = 0;
            for (int i = 0; i < (v - min) / width; i ++ ) {
                items += intArr[i];
            }
            System.out.println(items);
            System.out.println(all);
            res = (double) items / all;
            res += (double) (intArr[(v - min) / width]) / (width * all) * (v - (v - min) / width * width);
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            if (v < min) return 0;
            if (v > max) return 1;
            int items = 0;
            for (int i = 0; i < (v - min) / width; i ++ ) {
                items += intArr[i];
            }
            res = (double) items / all;
            res += (double) (intArr[(v - min) / width]) / (width * all) * (v - (v - min) / width * width) + (double) (intArr[(v - min) / width]) / (width * all);
        }
        return Double.min(res, 1);
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String res = "the initHistogram is ";
        for (int i = 0; i < intArr.length; i ++ ) {
            res += intArr[i];
            res += " ";
        }
        res = res + "min is " + min + " max is " + max;
        return res;
    }


}
