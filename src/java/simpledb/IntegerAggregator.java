package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
	
	private class IntPair {
		public int first;
		public int second;
		
		public IntPair(int first, int second) {
			this.first = first;
			this.second = second;
		}
	}

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, IntPair> aggregatorGroups;
    private IntPair aggregatorNoGroups;
    private String gbColName;
    private String aggColName;
    
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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregatorGroups = new HashMap<Field, IntPair>();
        this.aggregatorNoGroups = null;
        this.gbColName = null;
        this.aggColName = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (this.gbfield == NO_GROUPING) {
        	if (this.aggColName == null) {
        		setAggColName(tup);
        	}
        	if (this.aggregatorNoGroups == null) {
        		this.aggregatorNoGroups = createIntPairFromTuple(tup);
        	} else {
            	this.aggregatorNoGroups = mergeTupleIntoIntPair(this.aggregatorNoGroups, tup);
        	}
        } else {
        	if (this.aggColName == null || this.gbColName == null) {
        		setAggColName(tup);
        		setgbColName(tup);
        	}
        	Field tupGroupByField = tup.getField(this.gbfield);
        	if (!this.aggregatorGroups.containsKey(tupGroupByField)) {
        		this.aggregatorGroups.put(tupGroupByField, createIntPairFromTuple(tup));
        	} else {
        		IntPair currIntPair = this.aggregatorGroups.get(tupGroupByField);
        		this.aggregatorGroups.put(tupGroupByField, mergeTupleIntoIntPair(currIntPair, tup));
        	}
        }
    }
    
    private void setAggColName(Tuple tup) {
    	this.aggColName = tup.getTupleDesc().getFieldName(this.afield);
    }
    
    private void setgbColName(Tuple tup) {
    	this.gbColName = tup.getTupleDesc().getFieldName(this.gbfield);
    }
    
    private IntPair mergeTupleIntoIntPair(IntPair currIntPair, Tuple tup) {
    	int currTupleAggValue = ((IntField) tup.getField(this.afield)).getValue();
    	switch(this.what) {
		case AVG:
			int newIntPairSum = currIntPair.first + currTupleAggValue;
			return new IntPair(newIntPairSum, currIntPair.second + 1);
		case COUNT:
			return new IntPair(currIntPair.first + 1, 0);
		case MAX:
			int max = currTupleAggValue > currIntPair.first ? currTupleAggValue : currIntPair.first;
			return new IntPair(max, 0);
		case MIN:
			int min = currTupleAggValue < currIntPair.first ? currTupleAggValue : currIntPair.first;
			return new IntPair(min, 0);
		case SUM:
			return new IntPair(currIntPair.first + currTupleAggValue, 0);
		case SUM_COUNT:
			return new IntPair(currIntPair.first + currTupleAggValue, currIntPair.second + 1);
		case SC_AVG:
			if (this.gbfield == NO_GROUPING) {
				int sum = ((IntField) tup.getField(0)).getValue();
				int count = ((IntField) tup.getField(1)).getValue();
				return new IntPair(sum + currIntPair.first, count + currIntPair.second);
			} else {
				int sum = ((IntField) tup.getField(1)).getValue();
				int count = ((IntField) tup.getField(2)).getValue();
				return new IntPair(sum + currIntPair.first, count + currIntPair.second);
			}
		default:
			return null;
    	}
    }
    
    private IntPair createIntPairFromTuple(Tuple tup) {
    	int currTupleAggValue = ((IntField) tup.getField(this.afield)).getValue();
    	switch(this.what) {
		case AVG:
			return new IntPair(currTupleAggValue, 1);
		case COUNT:
			return new IntPair(1, 0);
		case MAX:
			return new IntPair(currTupleAggValue, 0);
		case MIN:
			return new IntPair(currTupleAggValue, 0);
		case SUM:
			return new IntPair(currTupleAggValue, 0);
		case SUM_COUNT:
			return new IntPair(currTupleAggValue, 1);
		case SC_AVG:
			if (this.gbfield == NO_GROUPING) {
				int sum = ((IntField) tup.getField(0)).getValue();
				int count = ((IntField) tup.getField(1)).getValue();
				return new IntPair(sum, count);
			} else {
				int sum = ((IntField) tup.getField(1)).getValue();
				int count = ((IntField) tup.getField(2)).getValue();
				return new IntPair(sum, count);
			}
		default:
			return null;
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	if (this.gbfield == NO_GROUPING) {
    		if (this.aggregatorNoGroups == null) {
    			return new TupleIterator(null, new HashSet<Tuple>());
    		}

    		if (this.what == Op.SUM_COUNT) {
    			Type[] tdTypes = { Type.INT_TYPE, Type.INT_TYPE };
    			String[] names = { "sum", "count" };
        		TupleDesc td = new TupleDesc(tdTypes, names);
        		Tuple aggTuple = getNoGroupTuple(this.aggregatorNoGroups, td);
        		Set<Tuple> iterableTuples = new HashSet<Tuple>();
        		iterableTuples.add(aggTuple);
        		return new TupleIterator(td, iterableTuples);
    		} else {
    			Type[] tdTypes = { Type.INT_TYPE };
    			String[] names = { this.aggColName };
        		TupleDesc td = new TupleDesc(tdTypes, names);
        		Tuple aggTuple = getNoGroupTuple(this.aggregatorNoGroups, td);
        		Set<Tuple> iterableTuples = new HashSet<Tuple>();
        		iterableTuples.add(aggTuple);
        		return new TupleIterator(td, iterableTuples);
    		}
    	} else {
    		if (this.aggregatorGroups.isEmpty()) {
    			return new TupleIterator(null, new HashSet<Tuple>());
    		}
    		if (this.what == Op.SUM_COUNT) {
        		Type[] tdTypes = { this.gbfieldtype, Type.INT_TYPE, Type.INT_TYPE };
        		String[] names = { this.gbColName, "sum", "count" };
        		TupleDesc td = new TupleDesc(tdTypes, names);
        		Set<Tuple> iterableTuples = new HashSet<Tuple>();
        		for (Field group : this.aggregatorGroups.keySet()) {
        			IntPair groupIntPair = this.aggregatorGroups.get(group);
        			Tuple aggTuple = getGroupsTuple(groupIntPair, td, group);
        			iterableTuples.add(aggTuple);
        		}
        		return new TupleIterator(td, iterableTuples);
    		} else {
        		Type[] tdTypes = { this.gbfieldtype, Type.INT_TYPE };
        		String[] names = { this.gbColName, this.aggColName };
        		TupleDesc td = new TupleDesc(tdTypes, names);
        		Set<Tuple> iterableTuples = new HashSet<Tuple>();
        		for (Field group : this.aggregatorGroups.keySet()) {
        			IntPair groupIntPair = this.aggregatorGroups.get(group);
        			Tuple aggTuple = getGroupsTuple(groupIntPair, td, group);
        			iterableTuples.add(aggTuple);
        		}
        		return new TupleIterator(td, iterableTuples);
    		}
    	}
    }
    
    private Tuple getNoGroupTuple(IntPair ip, TupleDesc td) {
		Tuple aggTuple = new Tuple(td);
		if (this.what != Op.SUM_COUNT) {
			IntField aggField = new IntField(getAggValue(ip));
			aggTuple.setField(0, aggField);
		} else {
			aggTuple.setField(0, new IntField(ip.first));
			aggTuple.setField(1, new IntField(ip.second));
		}

		return aggTuple;
    }
    
    private Tuple getGroupsTuple(IntPair ip, TupleDesc td, Field group) {
		Tuple aggTuple = new Tuple(td);
		aggTuple.setField(0, group);
		if (this.what != Op.SUM_COUNT) {
			IntField aggField = new IntField(getAggValue(ip));
			aggTuple.setField(1, aggField);
		} else {
			aggTuple.setField(1, new IntField(ip.first));
			aggTuple.setField(2, new IntField(ip.second));
		}
		return aggTuple;
    }
    
    private int getAggValue(IntPair ip) {
    	switch(this.what) {
		case AVG:
			return (ip.first / ip.second);
		case SC_AVG:
			return (ip.first / ip.second); 
		default:
			return ip.first;
    	}
    }
}