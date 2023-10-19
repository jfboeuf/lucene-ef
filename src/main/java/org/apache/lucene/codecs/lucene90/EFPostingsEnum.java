/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;

import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/** EFPostingsEnum */
public class EFPostingsEnum extends PostingsEnum {
	private final IndexInput startDocIn;
	private IndexInput lows, highs;
	private int docFreq;
	private final int maxDoc;
	private int doc;
	private int pos;
	private long lowBuffer;
	private long highBuffer;
	private long currentHigh;
	private long lowMask;
	private int numLowBits;
	private int highBitsShift;

	public EFPostingsEnum(FieldInfo fieldInfo, EFPostingsReader efPostingsReader) {
		this(efPostingsReader.docIn, efPostingsReader.maxDoc());
	}

	public EFPostingsEnum(IndexInput docIn, int maxDoc) {
		startDocIn = docIn;
		this.maxDoc = maxDoc;
	}

	@Override
	public int freq() throws IOException {
		return 1;
	}

	@Override
	public int nextPosition() throws IOException {
		return 0;
	}

	@Override
	public int startOffset() throws IOException {
		return 0;
	}

	@Override
	public int endOffset() throws IOException {
		return 0;
	}

	@Override
	public BytesRef getPayload() throws IOException {
		return null;
	}

	@Override
	public int docID() {
		return doc;
	}

	@Override
	public int nextDoc() throws IOException {
		if (pos < docFreq) {
			doc = (int) (readHigh() | readLow());
			pos++;
			return doc;
		}
		return doc = NO_MORE_DOCS;
	}

	long readHigh() throws IOException {
		long highPos = pos + currentHigh + highBitsShift;
		if (0l == (highPos & 63l) && 0 != highPos)
			highBuffer = highs.readLong();
		long high = highBuffer >>> highPos;
		while (0l == high) {
			long z = Long.SIZE - (highPos & 63l);
			currentHigh += z;
			highPos += z;
			highBuffer = highs.readLong();
			high = highBuffer >>> highPos;
		}
		currentHigh += Long.numberOfTrailingZeros(high);
		return currentHigh << numLowBits;
	}

	private long readLow() throws IOException {
		if (numLowBits == 0)
			return 0;
		int bitPos = (numLowBits * pos) & 63;
		long low = lowBuffer >>> bitPos;
		if ((bitPos + numLowBits) > 63) {
			lowBuffer = lows.readLong();
			low |= (lowBuffer << (Long.SIZE - bitPos));
		}
		return low & lowMask;
	}

	@Override
	public int advance(int target) throws IOException {
		int h = target >> numLowBits;
		while (true) {
			long highPos = (pos + currentHigh + highBitsShift);
			long high = highBuffer >> highPos;
			long delta = Long.bitCount(high);
			if(currentHigh+delta>=h) {
				
				break;
			}
		}
		while (doc < target) {
			nextDoc();
		}
		return doc;
	}

	@Override
	public long cost() {
		return docFreq;
	}

	public PostingsEnum reset(IntBlockTermState termState, int flags) throws IOException {
		docFreq = termState.docFreq;
		long docTermStartFP = termState.docStartFP;
		// skipOffset = termState.skipOffset;
		// singletonDocID = termState.singletonDocID;
		numLowBits = EFEncoder.numLowBits(docFreq, maxDoc - 1l);
		lowMask = EFEncoder.lowBitsMask(numLowBits);
		highBitsShift = EFEncoder.highBitsShift(docFreq, numLowBits);

		if (docFreq > 1) {
			// lazy init
			lows = startDocIn.clone();
			lows.seek(docTermStartFP);
			lowBuffer = lows.readLong();
			int lowLongCount = ((numLowBits * docFreq) >> 6) << 3;
			if (0 == lowLongCount) {
				highBuffer = lowBuffer;
				highs = lows;
			} else if (1 == lowLongCount) {
				highBuffer = lows.readLong();
				highs = lows;
			} else {
				highs = startDocIn.clone();
				highs.seek(docTermStartFP + lowLongCount);
				highBuffer = highs.readLong();
			}
			highBuffer &= -1l << highBitsShift;
		}
		doc = -1;
		pos = 0;
		currentHigh = 0;
		return this;
	}
}
