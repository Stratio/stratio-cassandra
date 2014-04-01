package org.apache.cassandra.db.index.stratio.query;

import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.db.index.stratio.schema.CellsMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * A {@link Condition} that matches documents matching boolean combinations of other queries, e.g.
 * {@link MatchCondition}s, {@link RangeCondition}s or other {@link BooleanCondition}s.
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeName("boolean")
public class BooleanCondition extends Condition {

	@JsonProperty("must")
	private List<Condition> must;

	@JsonProperty("should")
	private List<Condition> should;

	@JsonProperty("not")
	private List<Condition> not;

	/**
	 * Returns a new {@link BooleanCondition} compound by the specified {@link Condition}s.
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}. If
	 *            {@code null}, then {@link DEFAULT_BOOST} is used as default.
	 * @param must
	 *            the mandatory {@link Condition}s.
	 * @param should
	 *            the optional {@link Condition}s.
	 * @param not
	 *            the mandatory not {@link Condition}s.
	 */
	@JsonCreator
	public BooleanCondition(@JsonProperty("boost") Float boost,
	                        @JsonProperty("must") List<Condition> must,
	                        @JsonProperty("should") List<Condition> should,
	                        @JsonProperty("not") List<Condition> not) {
		super(boost);
		this.must = must == null ? new LinkedList<Condition>() : must;
		this.should = should == null ? new LinkedList<Condition>() : should;
		this.not = not == null ? new LinkedList<Condition>() : not;
	}

	/**
	 * Returns the mandatory {@link Condition}s. It never returns {@code null}.
	 * 
	 * @return the mandatory {@link Condition}s. It never returns {@code null}.
	 */
	public List<Condition> getMust() {
		return must;
	}

	/**
	 * Returns the optional {@link Condition}s. It never returns {@code null}.
	 * 
	 * @return the optional {@link Condition}s. It never returns {@code null}.
	 */
	public List<Condition> getShould() {
		return should;
	}

	/**
	 * Returns the mandatory not {@link Condition}s. It never returns {@code null}.
	 * 
	 * @return the mandatory not {@link Condition}s. It never returns {@code null}.
	 */
	public List<Condition> getNot() {
		return not;
	}

	/**
	 * Adds the specified {@link Condition} as mandatory.
	 * 
	 * @param condition
	 *            the {@link Condition} to be added as mandatory.
	 * @return this.
	 */
	public BooleanCondition must(Condition condition) {
		must.add(condition);
		return this;
	}

	/**
	 * Adds the specified {@link Condition} as optional.
	 * 
	 * @param condition
	 *            the {@link Condition} to be added as optional.
	 * @return this.
	 */
	public BooleanCondition should(Condition condition) {
		should.add(condition);
		return this;
	}

	/**
	 * Adds the specified {@link Condition} as mandatory not matching.
	 * 
	 * @param condition
	 *            the {@link Condition} to be added as mandatory not matching.
	 * @return this.
	 */
	public BooleanCondition not(Condition condition) {
		not.add(condition);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void analyze(Analyzer analyzer) {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Query query(CellsMapper cellsMapper) {
		org.apache.lucene.search.BooleanQuery luceneQuery = new org.apache.lucene.search.BooleanQuery();
		luceneQuery.setBoost(boost);
		for (Condition query : must) {
			luceneQuery.add(query.query(cellsMapper), Occur.MUST);
		}
		for (Condition query : should) {
			luceneQuery.add(query.query(cellsMapper), Occur.SHOULD);
		}
		for (Condition query : not) {
			luceneQuery.add(query.query(cellsMapper), Occur.MUST_NOT);
		}
		return luceneQuery;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getClass().getSimpleName());
		builder.append(" [boost=");
		builder.append(boost);
		builder.append(", must=");
		builder.append(must);
		builder.append(", should=");
		builder.append(should);
		builder.append(", not=");
		builder.append(not);
		builder.append("]");
		return builder.toString();
	}

}
