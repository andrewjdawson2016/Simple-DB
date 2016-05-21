package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    // <silentstrip lab1|lab2|lab3|lab4|lab5>
//    private String tname;
    private final int baseTups;
    private final int basePages;
    private final int costPerPageIO;
    private final Object[] histograms;
    private final int[] maxs, mins;
    private final TupleDesc td;
    // </silentstrip>

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // <insert lab1|lab2|lab3|lab4|lab5>
        // // For this function, you'll have to get the
        // // DbFile for the table in question,
        // // then scan through its tuples and calculate
        // // the values that you need.
        // // You should try to do this reasonably efficiently, but you don't
        // // necessarily have to (for example) do everything
        // // in a single scan of the table.
        // </insert>
        // <strip lab1|lab2|lab3|lab4|lab5>
        DbFile f = Database.getCatalog().getDatabaseFile(tableid);
        td = f.getTupleDesc();

        if (!(f instanceof HeapFile)) {
            basePages = 0;
            baseTups = 0;
            this.costPerPageIO=ioCostPerPage;
            histograms=null;
            mins=maxs=null;
            return;
        }

        costPerPageIO = ioCostPerPage;
        histograms = new Object[td.numFields()];
        maxs = new int[td.numFields()];
        mins = new int[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            maxs[i] = Integer.MIN_VALUE;
            mins[i] = Integer.MAX_VALUE;
        }
        // scan the data once to determine the min and max valus
        try {
            Transaction t = new Transaction();
            t.start();
            SeqScan s = new SeqScan(t.getId(), tableid, "t");
            s.open();
            while (s.hasNext()) {
                Tuple tup = s.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int v = ((IntField) tup.getField(i)).getValue();
                        if (v > maxs[i])
                            maxs[i] = v;
                        if (v < mins[i])
                            mins[i] = v;
                    }
                }
            }
            t.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < td.numFields(); i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                histograms[i] = new IntHistogram(NUM_HIST_BINS, mins[i],
                        maxs[i]);
            } else {
                histograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }

        basePages = ((HeapFile) f).numPages();
        int count = 0;
        try {
            Transaction t = new Transaction();
            t.start();
            SeqScan s = new SeqScan(t.getId(), tableid, "t");
            s.open();
            while (s.hasNext()) { // scan again to populate histograms
                Tuple tup = s.next();
                count++;
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int v = ((IntField) tup.getField(i)).getValue();
                        ((IntHistogram) histograms[i]).addValue(v);
                    } else {
                        String v = ((StringField) tup.getField(i)).getValue();
                        ((StringHistogram) histograms[i]).addValue(v);
                    }
                }

            }
            t.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        baseTups = count;

        // </strip>
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // <strip lab1|lab2|lab3|lab4|lab5>
        return basePages * costPerPageIO;
        // </strip>
        // <insert lab1|lab2|lab3|lab4|lab5>
        // return 0;
        // </insert>
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // <strip lab1|lab2|lab3|lab4|lab5>
        return (int) (baseTups * selectivityFactor);
        // </strip>
        // <insert lab1|lab2|lab3|lab4|lab5>
        // return 0;
        // </insert>
    }

    
    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // <strip lab1|lab2|lab3|lab4|lab5>
        if (op == Predicate.Op.EQUALS) {
            if (this.histograms != null)
                if (td.getFieldType(field) == Type.INT_TYPE) {
                    return ((IntHistogram) this.histograms[field])
                            .avgSelectivity();
                } else if (td.getFieldType(field) == Type.STRING_TYPE) {
                    return ((StringHistogram) this.histograms[field])
                            .avgSelectivity();
                }
        }
        return 0.5; // make something up
        // </strip>
        // <insert lab1|lab2|lab3|lab4|lab5>
        // return 1.0;
        // </insert>
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // <strip lab1|lab2|lab3|lab4|lab5>
        if (histograms != null) {
            if (td.getFieldType(field) == Type.INT_TYPE) {
                IntHistogram hist = (IntHistogram) histograms[field];
                double sel = hist.estimateSelectivity(op,
                        ((IntField) constant).getValue());
//                System.out.println("SELECTIVITY OF PREDICATE " + field + " "
//                        + op + " " + constant + " IS " + sel);
                return sel;
            } else {
                StringHistogram hist = (StringHistogram) histograms[field];
                double sel = hist.estimateSelectivity(op,
                        ((StringField) constant).getValue());
//                System.out.println("SELECTIVITY OF PREDICATE " + field + " "
//                        + op + " " + constant + " IS " + sel);

                return sel;
            }
        }
        return .5; // make something up.
        // </strip>
        // <insert lab1|lab2|lab3|lab4|lab5>
        // return 1.0;
        // </insert>
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // <strip lab1|lab2|lab3|lab4|lab5>
        return this.baseTups;
        // </strip>
        // <insert lab1|lab2|lab3|lab4|lab5>
        // return 0;
        // </insert>
    }

}
