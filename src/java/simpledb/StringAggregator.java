package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> aggregatorGroups;
    private Integer aggregatorNoGroups;
    private String gbColName;
    private String aggColName;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
        	throw new IllegalArgumentException();
        }
        
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregatorGroups = new HashMap<Field, Integer>();
        this.aggregatorNoGroups = null;
        this.gbColName = null;
        this.aggColName = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (this.gbfield == NO_GROUPING) {
        	if (this.aggColName == null) {
        		setAggColName(tup);
        	}
        	if (this.aggregatorNoGroups == null) {
        		this.aggregatorNoGroups = 1;
        	} else {
            	this.aggregatorNoGroups = this.aggregatorNoGroups + 1;
        	}
        } else {
        	if (this.aggColName == null || this.gbColName == null) {
        		setAggColName(tup);
        		setgbColName(tup);
        	}
        	Field tupGroupByField = tup.getField(this.gbfield);
        	if (!this.aggregatorGroups.containsKey(tupGroupByField)) {
        		this.aggregatorGroups.put(tupGroupByField, 1);
        	} else {
        		int currCount = this.aggregatorGroups.get(tupGroupByField);
        		this.aggregatorGroups.put(tupGroupByField, currCount + 1);
        	}
        }
    }
    
    private void setAggColName(Tuple tup) {
    	this.aggColName = tup.getTupleDesc().getFieldName(this.afield);
    }
    
    private void setgbColName(Tuple tup) {
    	this.gbColName = tup.getTupleDesc().getFieldName(this.gbfield);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	if (this.gbfield == NO_GROUPING) {
    		if (this.aggregatorNoGroups == null) {
    			return new TupleIterator(null, new HashSet<Tuple>());
    		}
    		Type[] tdTypes = { Type.INT_TYPE };
    		String[] names = { this.aggColName };
    		TupleDesc td = new TupleDesc(tdTypes, names);
    		Tuple aggTuple = getNoGroupTuple(this.aggregatorNoGroups, td);
    		Set<Tuple> iterableTuples = new HashSet<Tuple>();
    		iterableTuples.add(aggTuple);
    		return new TupleIterator(td, iterableTuples);
    	} else {
    		if (this.aggregatorGroups.isEmpty()) {
    			return new TupleIterator(null, new HashSet<Tuple>());
    		}
    		Type[] tdTypes = { this.gbfieldtype, Type.INT_TYPE };
    		String[] names = { this.gbColName, this.aggColName };
    		TupleDesc td = new TupleDesc(tdTypes, names);
    		Set<Tuple> iterableTuples = new HashSet<Tuple>();
    		for (Field group : this.aggregatorGroups.keySet()) {
    			int groupCount = this.aggregatorGroups.get(group);
    			Tuple aggTuple = getGroupsTuple(groupCount, td, group);
    			iterableTuples.add(aggTuple);
    		}
    		return new TupleIterator(td, iterableTuples);
    	}
    }
    
    private Tuple getNoGroupTuple(int count, TupleDesc td) {
		Tuple aggTuple = new Tuple(td);
		IntField aggField = new IntField(count);
		aggTuple.setField(0, aggField);
		return aggTuple;
    }
    
    private Tuple getGroupsTuple(int count, TupleDesc td, Field group) {
		Tuple aggTuple = new Tuple(td);
		aggTuple.setField(0, group);
		IntField aggField = new IntField(count);
		aggTuple.setField(1, aggField);
		return aggTuple;
    }
}