/*
* Copyright 2014, Stratio.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.cassandra.db.index.stratio;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.stratio.util.ByteBufferUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

public class ColumnsFilter extends Filter {

	private final ByteBuffer min;
	private final ByteBuffer max;
	private final AbstractType<?> columnNameType;

	public ColumnsFilter(ByteBuffer min, ByteBuffer max, AbstractType<?> columnNameType) {
		super();
		this.min = min;
		this.max = max;
		this.columnNameType = columnNameType;
	}

	@Override
	public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
		AtomicReader atomicReader = context.reader();
		Bits liveDocs = atomicReader.getLiveDocs();

		OpenBitSet bitSet = new OpenBitSet(atomicReader.maxDoc());

		Terms terms = atomicReader.terms(ClusteringKeyMapper.FIELD_NAME);
		if (terms == null) {
			return null;
		}

		final TermsEnum termsEnum = terms.iterator(null);

		DocsEnum docsEnum = null;
		BytesRef bytesRef = termsEnum.next();

		while (bytesRef != null) {
			String string = bytesRef.utf8ToString();
			ByteBuffer value = ByteBufferUtils.fromString(string);
			boolean accepted = true;
			if (min != null && !ByteBufferUtils.isEmpty(min)) {
				accepted &= columnNameType.compare(min, value) <= 0;
			}
			if (max != null && !ByteBufferUtils.isEmpty(max)) {
				accepted &= columnNameType.compare(max, value) >= 0;
			}

			docsEnum = termsEnum.docs(liveDocs, docsEnum);
			if (accepted) {
				Integer docID = docsEnum.nextDoc();
				while (docID != DocIdSetIterator.NO_MORE_DOCS) {
					bitSet.set(docID);
					docID = docsEnum.nextDoc();
				}
			}
			bytesRef = termsEnum.next();
		}
		return bitSet;
	}

}
