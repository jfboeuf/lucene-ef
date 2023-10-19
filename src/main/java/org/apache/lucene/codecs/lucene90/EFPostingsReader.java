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
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.IOUtils;

/** EF Posting reader */
public class EFPostingsReader extends PostingsReaderBase {
  final IndexInput docIn;
  final SegmentReadState segmentState;

  public EFPostingsReader(SegmentReadState state) throws IOException {
    boolean success = false;
    IndexInput docIn = null;
    IndexInput posIn = null;
    IndexInput payIn = null;
    segmentState = state;
    // NOTE: these data files are too costly to verify checksum against all the bytes on open,
    // but for now we at least verify proper structure of the checksum footer: which looks
    // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
    // such as file truncation.
    String docName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix, Lucene90PostingsFormat.DOC_EXTENSION);
    try {
      docIn = state.directory.openInput(docName, state.context);
      CodecUtil.checkIndexHeader(
          docIn,
          EFPostingsWriter.DOC_CODEC,
          EFPostingsWriter.VERSION_CURRENT,
          EFPostingsWriter.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.retrieveChecksum(docIn);

      this.docIn = docIn;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docIn, posIn, payIn);
      }
    }
  }

  @Override
  public void init(IndexInput termsIn, SegmentReadState state) throws IOException {
    CodecUtil.checkIndexHeader(
        termsIn,
        "EFPostingsWriterTerms",
        EFPostingsWriter.VERSION_CURRENT,
        EFPostingsWriter.VERSION_CURRENT,
        state.segmentInfo.getId(),
        state.segmentSuffix);
  }

  @Override
  public BlockTermState newTermState() throws IOException {
    return new IntBlockTermState();
  }

  @Override
  public void decodeTerm(
      DataInput in, FieldInfo fieldInfo, BlockTermState _termState, boolean absolute)
      throws IOException {
    final IntBlockTermState termState = (IntBlockTermState) _termState;
    final boolean fieldHasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    final boolean fieldHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    final boolean fieldHasPayloads = fieldInfo.hasPayloads();

    if (absolute) {
      termState.docStartFP = 0;
      termState.posStartFP = 0;
      termState.payStartFP = 0;
    }

    final long l = in.readVLong();
    if ((l & 0x01) == 0) {
      termState.docStartFP += l >>> 1;
      if (termState.docFreq == 1) {
        termState.singletonDocID = in.readVInt();
      } else {
        termState.singletonDocID = -1;
      }
    } else {
      assert absolute == false;
      assert termState.singletonDocID != -1;
      termState.singletonDocID += BitUtil.zigZagDecode(l >>> 1);
    }

    termState.skipOffset = -1;
  }

  @Override
  public PostingsEnum postings(
      FieldInfo fieldInfo, BlockTermState termState, PostingsEnum reuse, int flags) throws IOException {
    EFPostingsEnum docsEnum = new EFPostingsEnum(fieldInfo, this);
    return docsEnum.reset((IntBlockTermState) termState, flags);
  }

  @Override
  public ImpactsEnum impacts(FieldInfo fieldInfo, BlockTermState state, int flags)
      throws IOException {
    return new SlowImpactsEnum(postings(fieldInfo, state, null, flags));
  }

  @Override
  public void checkIntegrity() throws IOException {
    if (docIn != null) {
      CodecUtil.checksumEntireFile(docIn);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docIn);
  }

  public int maxDoc() {
    return segmentState.segmentInfo.maxDoc();
  }
}
