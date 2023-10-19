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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.Collections;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class TestEFBench {
	public interface ThrowingRunnable {
		public void run() throws Throwable;
	}

	private static final String FIELD = "myField";
	private static final int DOC_COUNT = 5_000_000;

	public static void main(String[] args) throws Throwable {
		new TestEFBench().compareCodecs();
	}

	public void compareCodecs() throws Throwable {
		try (Directory lucene95Dir = FSDirectory.open(Path.of("/home/jboeuf/Lucene95"));
				Directory efDir = FSDirectory.open(Path.of("/home/jboeuf/EFCodec"))) {
//			createIndexes(lucene95Dir, efDir);
			try (DirectoryReader lucene95Reader = DirectoryReader.open(lucene95Dir);
					DirectoryReader efReader = DirectoryReader.open(efDir)) {
				time("Check consistency: ", () -> checkConsistency(lucene95Reader, efReader));
				long ef = 0, l95 = 0;
				for (int i = 0; i < 100; i++) {
					ef += time("Crawl ef: ", () -> crawlPostings(efReader));
					l95 += time("Crawl lucene: ", () -> crawlPostings(lucene95Reader));
				}
				System.err.println("AVG: l95=" + (l95 / 100d) + " ef=" + (ef / 100d));
			}
		}
	}

	private void checkConsistency(DirectoryReader lucene95Reader, DirectoryReader efReader) throws IOException {
		assertEquals(lucene95Reader.numDocs(), efReader.numDocs());
		TermsEnum lucene95Terms = MultiTerms.getTerms(lucene95Reader, FIELD).iterator();
		TermsEnum efTerms = MultiTerms.getTerms(efReader, FIELD).iterator();
		while (true) {
			BytesRef lucene95 = lucene95Terms.next();
			BytesRef ef = efTerms.next();
			assertEquals(lucene95, ef);
			if (null == lucene95)
				break;
			int docFreq = lucene95Terms.docFreq();
			if (1 == docFreq) {
				continue;
			}
			PostingsEnum l95Postings = lucene95Terms.postings(null);
			PostingsEnum efPostings = efTerms.postings(null);
			while (true) {
				int l95Doc = l95Postings.nextDoc();
				int efDoc = efPostings.nextDoc();
				assertEquals(l95Doc, efDoc);
				if (DocIdSetIterator.NO_MORE_DOCS == l95Doc) {
					break;
				}
			}
		}
	}

	private void crawlPostings(DirectoryReader reader) throws IOException {
		TermsEnum terms = MultiTerms.getTerms(reader, FIELD).iterator();
		while (true) {
			BytesRef lucene95 = terms.next();
			if (null == lucene95)
				break;
			if (1 == terms.docFreq())
				continue;
			PostingsEnum postings = terms.postings(null);
			while (true) {
				int doc = postings.nextDoc();
				if (DocIdSetIterator.NO_MORE_DOCS == doc)
					break;
			}
		}
	}

	void createIndexes(Directory lucene95Dir, Directory efDir) throws IOException, Throwable, FileNotFoundException {
		try (IndexWriter lucene95Writer = createWriter(new Lucene95Codec(), lucene95Dir);
				IndexWriter efWriter = createWriter(new EFCodec(), efDir);) {
			File f = new File("/home/jboeuf/testData/data");
			Field field = createField();
			Iterable<Field> doc = Collections.singleton(field);
			try (Reader fr = new FileReader(f); BufferedReader br = new BufferedReader(fr)) {
				for (int i = 0; i < DOC_COUNT; i++) {
					String str = br.readLine();
					field.setStringValue(str);
					lucene95Writer.addDocument(doc);
					efWriter.addDocument(doc);
				}
				time("Commit Lucene95: ", lucene95Writer::commit);
				time("Commit ef: ", efWriter::commit);
			}
		}
	}

	private long time(String msg, ThrowingRunnable action) throws Throwable {
		long t0 = System.currentTimeMillis();
		action.run();
		t0 = System.currentTimeMillis() - t0;
		System.err.println(msg + t0);
		return t0;
	}

	IndexWriter createWriter(Codec codec, Directory dir) throws IOException {
		IndexWriterConfig conf = new IndexWriterConfig();
		conf.setCodec(codec);
		conf.setUseCompoundFile(false);
		conf.setRAMBufferSizeMB(5000d);
		return new IndexWriter(dir, conf);
	}

	public Field createField() {
		FieldType type = new FieldType();
		type.setIndexOptions(IndexOptions.DOCS);
		type.setTokenized(true);
		type.freeze();
		return new Field(FIELD, "", type);
	}
}
