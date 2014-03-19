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
 * A {@link AbstractQuery} that matches documents matching boolean combinations of other queries,
 * e.g. {@link MatchQuery}s, {@link RangeQuery}s or other {@link BooleanQuery}s.
 * 
 * @version 0.1
 * @author adelapena
 */
@JsonTypeName("boolean")
public class BooleanQuery extends AbstractQuery {

	@JsonProperty("must")
	private List<AbstractQuery> must;

	@JsonProperty("should")
	private List<AbstractQuery> should;

	@JsonProperty("not")
	private List<AbstractQuery> not;

	/**
	 * Returns a new {@link BooleanQuery} compound by the specified {@link AbstractQuery}s.
	 * 
	 * @param boost
	 *            The boost for this query clause. Documents matching this clause will (in addition
	 *            to the normal weightings) have their score multiplied by {@code boost}.
	 * @param must
	 *            the mandatory {@link AbstractQuery}s.
	 * @param should
	 *            the optional {@link AbstractQuery}s.
	 * @param not
	 *            the mandatory not {@link AbstractQuery}s.
	 */
	@JsonCreator
	public BooleanQuery(@JsonProperty("boost") Float boost,
	                    @JsonProperty("must") List<AbstractQuery> must,
	                    @JsonProperty("should") List<AbstractQuery> should,
	                    @JsonProperty("not") List<AbstractQuery> not) {
		super(boost);
		this.must = must == null ? new LinkedList<AbstractQuery>() : must;
		this.should = should == null ? new LinkedList<AbstractQuery>() : should;
		this.not = not == null ? new LinkedList<AbstractQuery>() : not;
	}

	/**
	 * Returns the mandatory {@link AbstractQuery}s. It never returns {@code null}.
	 * 
	 * @return the mandatory {@link AbstractQuery}s. It never returns {@code null}.
	 */
	public List<AbstractQuery> getMust() {
		return must;
	}

	/**
	 * Returns the optional {@link AbstractQuery}s. It never returns {@code null}.
	 * 
	 * @return the optional {@link AbstractQuery}s. It never returns {@code null}.
	 */
	public List<AbstractQuery> getShould() {
		return should;
	}

	/**
	 * Returns the mandatory not {@link AbstractQuery}s. It never returns {@code null}.
	 * 
	 * @return the mandatory not {@link AbstractQuery}s. It never returns {@code null}.
	 */
	public List<AbstractQuery> getNot() {
		return not;
	}

	/**
	 * Adds the specified {@link AbstractQuery} as mandatory.
	 * 
	 * @param abstractQuery
	 *            the {@link AbstractQuery} to be added as mandatory.
	 * @return this.
	 */
	public BooleanQuery must(AbstractQuery abstractQuery) {
		must.add(abstractQuery);
		return this;
	}

	/**
	 * Adds the specified {@link AbstractQuery} as optional.
	 * 
	 * @param abstractQuery
	 *            the {@link AbstractQuery} to be added as optional.
	 * @return this.
	 */
	public BooleanQuery should(AbstractQuery abstractQuery) {
		should.add(abstractQuery);
		return this;
	}

	/**
	 * Adds the specified {@link AbstractQuery} as mandatory not matching.
	 * 
	 * @param abstractQuery
	 *            the {@link AbstractQuery} to be added as mandatory not matching.
	 * @return this.
	 */
	public BooleanQuery not(AbstractQuery abstractQuery) {
		not.add(abstractQuery);
		return this;
	}

	@Override
	public void analyze(Analyzer analyzer) {
	}

	@Override
	public Query toLucene(CellsMapper cellsMapper) {
		org.apache.lucene.search.BooleanQuery luceneQuery = new org.apache.lucene.search.BooleanQuery();
		luceneQuery.setBoost(boost);
		for (AbstractQuery query : must) {
			luceneQuery.add(query.toLucene(cellsMapper), Occur.MUST);
		}
		for (AbstractQuery query : should) {
			luceneQuery.add(query.toLucene(cellsMapper), Occur.SHOULD);
		}
		for (AbstractQuery query : not) {
			luceneQuery.add(query.toLucene(cellsMapper), Occur.MUST_NOT);
		}
		return luceneQuery;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("BooleanQuery [must=");
		builder.append(must);
		builder.append(", should=");
		builder.append(should);
		builder.append(", not=");
		builder.append(not);
		builder.append("]");
		return builder.toString();
	}

}
