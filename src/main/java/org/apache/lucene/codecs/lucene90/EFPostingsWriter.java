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

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/** EF posting writer. */
public class EFPostingsWriter extends PushPostingsWriterBase {
  static final IntBlockTermState emptyState = new IntBlockTermState();

  static final String DOC_CODEC = "EFPostingFormat";

  static final int VERSION_CURRENT = 0;

  private IntBlockTermState lastState;
  private final EFEncoder docEncoder;
  private IndexOutput docOut;

  public EFPostingsWriter(SegmentWriteState state) throws IOException {
    String docFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix, Lucene90PostingsFormat.DOC_EXTENSION);
    docOut = state.directory.createOutput(docFileName, state.context);
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(
          docOut, DOC_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      docOut.alignFilePointer(Long.BYTES);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut);
      }
    }
    docEncoder = new EFEncoder(docOut, state.segmentInfo.maxDoc());
  }

  @Override
  public BlockTermState newTermState() throws IOException {
    return new IntBlockTermState();
  }

  @Override
  public void startTerm(NumericDocValues norms) throws IOException {
    docEncoder.startTerm();
  }

  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    //    assert state.docFreq == docCount : state.docFreq + " vs " + docCount;

    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to
    // it.
    final int singletonDocID = docEncoder.finish();
    state.docStartFP = docEncoder.getStartFilePointer();
    state.singletonDocID = singletonDocID;
    state.skipOffset = -1l;
    state.lastPosBlockOffset = -1l;
  }

  @Override
  public void startDoc(int docID, int freq) throws IOException {
    docEncoder.addDoc(docID, freq);
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset)
      throws IOException {}

  @Override
  public void finishDoc() throws IOException {}

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(
        termsOut,
        "EFPostingsWriterTerms",
        VERSION_CURRENT,
        state.segmentInfo.getId(),
        state.segmentSuffix);
  }

  @Override
  public void encodeTerm(
      DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute)
      throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    if (absolute) {
      lastState = emptyState;
      assert lastState.docStartFP == 0;
    }

    if (lastState.singletonDocID != -1
        && state.singletonDocID != -1
        && state.docStartFP == lastState.docStartFP) {
      // With runs of rare values such as ID fields, the increment of pointers in the docs file is
      // often 0.
      // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we
      // encode the delta
      // between consecutive doc IDs to save space.
      final long delta = (long) state.singletonDocID - lastState.singletonDocID;
      out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
    } else {
      out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);
      if (state.singletonDocID != -1) {
        out.writeVInt(state.singletonDocID);
      }
    }

    if (state.skipOffset != -1) {
      out.writeVLong(state.skipOffset);
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (docOut != null) {
        CodecUtil.writeFooter(docOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(docOut);
      } else {
        IOUtils.closeWhileHandlingException(docOut);
      }
      docOut = null;
    }
  }
}
