package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, List<Tuple>> aggMap;
    private List<Tuple> aggValue;
    
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
        this.aggMap = new HashMap<Field, List<Tuple>>();
        this.aggValue = new ArrayList<Tuple>();
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
        	this.aggValue.add(tup);
        } else {
        	updateAggMap(tup, tup.getField(this.gbfield));
        }
    }
    
    private void updateAggMap(Tuple tup, Field group) {
    	if (!this.aggMap.containsKey(group)) {
    		this.aggMap.put(group, new ArrayList<Tuple>());
    	}
    	this.aggMap.get(group).add(tup);
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
    		if (this.aggValue.isEmpty()) {
    			return new TupleIterator(null, new HashSet<Tuple>());
    		}
    		Type[] tdTypes = { Type.INT_TYPE };
    		String aggerateValName = this.aggValue.get(0).getTupleDesc().getFieldName(this.afield);
    		String[] names = { aggerateValName };
    		TupleDesc td = new TupleDesc(tdTypes, names);
    		Tuple aggTuple = new Tuple(td);
    		IntField aggField = new IntField(computeAggValue(this.aggValue));
    		aggTuple.setField(0, aggField);
    		Set<Tuple> resultTuples = new HashSet<Tuple>();
    		resultTuples.add(aggTuple);
    		return new TupleIterator(td, resultTuples);
    	} else {
    		if (this.aggMap.isEmpty()) {
    			return new TupleIterator(null, new HashSet<Tuple>());
    		}
    		Type[] tdTypes = { this.gbfieldtype, Type.INT_TYPE };
    		String[] names = getPairColNames();
    		TupleDesc td = new TupleDesc(tdTypes, names);
    		Set<Tuple> resultTuples = new HashSet<Tuple>();
    		for (Field group : this.aggMap.keySet()) {
    			IntField aggField = new IntField(computeAggValue(this.aggMap.get(group)));
    			Tuple currAggTuple = new Tuple(td);
    			if (group.getType() == Type.STRING_TYPE) {
    				currAggTuple.setField(0, (StringField) group);
    			} else {
    				currAggTuple.setField(0, (IntField) group);
    			}
        		currAggTuple.setField(1, aggField);
        		resultTuples.add(currAggTuple);
    		}
    		return new TupleIterator(td, resultTuples);
    	}
    }
    
    private String[] getPairColNames() {
    	Iterator<Field> itr = this.aggMap.keySet().iterator();
    	Tuple randomTuple = this.aggMap.get(itr.next()).get(0);
    	String[] result = new String[2];
    	result[0] = randomTuple.getTupleDesc().getFieldName(this.gbfield);
    	result[1] = randomTuple.getTupleDesc().getFieldName(this.afield);
    	return result;
    }
    
    private int computeAggValue(List<Tuple> tupleValues) {
    	List<Integer> afieldList = new ArrayList<Integer>();
    	for (Tuple currTuple : tupleValues) {
    		afieldList.add(((IntField) currTuple.getField(this.afield)).getValue());
    	}
    	switch(this.what) {
		case AVG:
			int sum = 0;
			for (int curr : afieldList) {
				sum += curr;
			}
			return (sum / afieldList.size());
		case COUNT:
			return afieldList.size();
		case MAX:
			int max = Integer.MIN_VALUE;
			for (int curr : afieldList) {
				if (curr > max) {
					max = curr;
				}
			}
			return max;
		case MIN:
			int min = Integer.MAX_VALUE;
			for (int curr : afieldList) {
				if (curr < min) {
					min = curr;
				}
			}
			return min;
		case SUM:
			int result = 0;
			for (int curr : afieldList) {
				result += curr;
			}
			return result;
		default:
			return -1;
    	}
    }
}