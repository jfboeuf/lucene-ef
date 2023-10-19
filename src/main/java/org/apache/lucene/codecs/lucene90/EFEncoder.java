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

import org.apache.lucene.store.IndexOutput;

/** EF Encoder */
public class EFEncoder {
  private static final int LOG2_LONG_SIZE = Long.numberOfTrailingZeros(Long.SIZE);
  private static final int LOG2_BYTE_SIZE = Long.numberOfTrailingZeros(Byte.SIZE);
  private static final int DEFAULT_PARTITION_SIZE = 1<<29;
  private final int partitionSize;
  private final int bufferPosMask;

  private final long[] docBuffer;
  private final long[] partitionsMax;
  private final long[] partitionsFP;

  private final IndexOutput docOut;

  private final int maxDoc;
  private int docCount;
  private long termStartFP;

  public EFEncoder(IndexOutput docOut, int maxDoc) {
    this(docOut, maxDoc, DEFAULT_PARTITION_SIZE);
  }

  public EFEncoder(IndexOutput docOut, int maxDoc, int size) {
    super();
    this.docOut = docOut;
    this.maxDoc = maxDoc;
    if (1 != Integer.bitCount(size))
      throw new IllegalArgumentException(
          "partition size must be a power of two (and ideally a multiple of 64)");
    partitionSize = size;
    int bufferSize = Math.min(maxDoc, size << 1);
    docBuffer = new long[bufferSize];
    int maxPart = maxDoc / partitionSize;
    partitionsMax = new long[maxPart];
    partitionsFP = new long[maxPart];
    bufferPosMask = (size << 1) - 1;
  }

  private void encode(long[] values, int offSet, int n, long max, long min) throws IOException {
    max = max - min;
//    if (max == n) {
////      System.err.println("ALL");
//      return;
//    }
//    if (max < n * 4 / 3) {
//      // TODO encode reverse & return;
//    }
//    if (max < 4 * n) {
////      System.err.println("DENSE");
//      encodeBS(values, offSet, n, min);
//      return;
//    }
    encodeEF(values, offSet, n, max - 1, min, bufferPosMask);
  }

  private void encodeBS(long[] values, int offSet, int n, long min) throws IOException {
    long bitset = 0;
    int currentWord = 0;
    for (int i = 0; i < n; i++) {
      long v = values[(i + offSet) & bufferPosMask] - min;
      long destWord = v >> 6;
      assert destWord >= currentWord;
      for (; currentWord != destWord; currentWord++) {
        docOut.writeLong(bitset);
        bitset = 0;
      }
      long bitmask = 1L << v;
      bitset |= bitmask;
    }
    docOut.writeLong(bitset);
  }

  private void encodeEF(long[] values, int offSet, int n, long max, long min, int valuesMask)
      throws IOException {
    int numLowBits = numLowBits(n, max);
    long lowMask = lowBitsMask(numLowBits);
    int trailingLowBitsCount = highBitsShift(n, numLowBits);
    // Trade off: we could save some bits by using values[(offSet + n - 1) & valuesMask] - min
    // as max to avoid writing trailing 0s but it eventually saves less than 1% and make decoding
    // much more complex and costly as we can assume how long we have to read from the file, we 
    // must count n 1s for this.
    long highBitsCount = trailingLowBitsCount + n + (max >>> numLowBits);
    long[] highBits = new long[numLongsForBits(highBitsCount)];
    long lowBits = 0;
    for (int i = 0; i < n; i++) {
      long value = values[(offSet + i) & valuesMask] - min;
      long high = value >>> numLowBits;
      long low = value & lowMask;
      lowBits = writeLow(i, low, lowBits, numLowBits);
      writeHigh(i + trailingLowBitsCount, high, highBits);
    }
//    System.err.println("lows: " + Long.toBinaryString(lowBits));
//    System.err.println("high: " + Long.toBinaryString(highBits[0]));
    highBits[0] |= lowBits;
    //    System.err.println("all: " + Long.toBinaryString(highBits[0]));
    for (int i = 0; i < highBits.length; i++) {
      docOut.writeLong(highBits[i]);
    }
    // Aligned on long costs about 5%
    // easier
    //    long trailingHighBitsCount = highBitsCount & 0x3F;
    //    long lastHigh = highBits[highBits.length - 1];
    //    int trailingBytesCount = numBytesForBits(trailingHighBitsCount);
    //    for (int i = 0; i <trailingBytesCount; i++) {
    //      docOut.writeByte((byte) (lastHigh >>> (i*Byte.SIZE)));
    //    }
  }

  static int highBitsShift(int n, int numLowBits) {
    return (n * numLowBits) & 0x3F;
  }

  static long lowBitsMask(int numLowBits) {
    return Long.MAX_VALUE >>> (Long.SIZE - 1 - numLowBits);
  }

  static int numLowBits(int n, long max) {
    return 63 - Long.numberOfLeadingZeros(max / n);
  }

  private static void writeHigh(int i, long highValue, long[] highBits) {
    long nextHighBitNum = i + highValue;
    highBits[(int) (nextHighBitNum >>> LOG2_LONG_SIZE)] |= (1L << (nextHighBitNum & (Long.SIZE - 1)));
  }

  private long writeLow(int i, long value, long lowBits, int numLowBits) throws IOException {
    if (numLowBits == 0)
      return 0;
    int bitPos = numLowBits * i;
    int bitPosAtIndex = bitPos & (Long.SIZE - 1);
    lowBits |= (value << bitPosAtIndex);
    if ((bitPosAtIndex + numLowBits) >= Long.SIZE) {
      docOut.writeLong(lowBits);
      lowBits = (value >>> (Long.SIZE - bitPosAtIndex));
    }
    return lowBits;
  }

  private static int numLongsForBits(
      long numBits) { // Note: int version in FixedBitSet.bits2words()
    assert numBits >= 0 : numBits;
    return (int) ((numBits + (Long.SIZE - 1)) >>> LOG2_LONG_SIZE);
  }

  private static int numBytesForBits(long numBits) {
    assert numBits >= 0 : numBits;
    return (int) ((numBits + (Byte.SIZE - 1)) >>> LOG2_BYTE_SIZE);
  }

  public long getStartFilePointer() {
    return termStartFP;
  }

  public void addDoc(int docID, int freq) throws IOException {
    int bufferPos = docCount++ & bufferPosMask;
    if (docCount % (partitionSize + 1) == partitionSize) {
      int p = docCount / (partitionSize + 1);
      if (0 < p) {
        --p;
        // encodes the previous partition
        int start = (p * (partitionSize + 1)) & bufferPosMask;
        int end = (start + partitionSize) & bufferPosMask;
        long min = 0 == p ? 0 : partitionsMax[p - 1] + 1;
        long max = docBuffer[end];
        encode(docBuffer, start, partitionSize, max, min);
        partitionsMax[p] = max;
        partitionsFP[p] = docOut.getFilePointer();
      }
    }
    docBuffer[bufferPos] = docID;
  }

  public int finish() throws IOException {
    int singleDoc = -1;
    if (docCount == 1) {
      singleDoc = (int) docBuffer[0];
    } else {
      int end = (docCount - 1) & bufferPosMask;
      int start;
      int count;
      long min;
      int p = (docCount - partitionSize) / (partitionSize + 1);
      if (docCount <= (partitionSize << 1)) {
        start = 0;
        count = docCount;
        min = 0;
      } else {
        count = partitionSize + (docCount + 1) % (partitionSize + 1);
        start = (end - count + 1) & bufferPosMask;
        min = 0 == p ? 0 : partitionsMax[p - 1] + 1;
      }
      // encodes everything in the previous partition
      encode(docBuffer, start, count, maxDoc, min);
      if (0 != p) {
        encodeEF(partitionsMax, 0, p, maxDoc - partitionSize, partitionSize + 1, -1);
        long maxSize = Math.min(
            docCount * (2 + 63 - Long.numberOfLeadingZeros(maxDoc / docCount)),
            4 * numBytesForBits(maxDoc));
        encodeEF(partitionsFP, 0, p, maxSize, termStartFP, -1);
      }
    }
    docCount = 0;
    return singleDoc;
  }

  void startTerm() {
    termStartFP = docOut.getFilePointer();
  }
}
