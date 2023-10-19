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
package jmh;

import java.io.IOException;

import org.apache.lucene.codecs.lucene90.EFPostingsEnum;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class JmhEFNext extends JmhEFPostingsBase {
	@Benchmark
	public void testNextReuse(Blackhole bh) throws IOException {
		crawlPostings(postings, bh);
	}

	@Benchmark
	public void testNextNew(Blackhole bh) throws IOException {
		EFPostingsEnum p = new EFPostingsEnum(ixIn, 10_000);
		crawlPostings(p, bh);
		bh.consume(p);
	}

	void crawlPostings(EFPostingsEnum p, Blackhole bh) throws IOException {
		p.reset(termState, 0);
		for (int i = 0; i < 10_000; i++) {
			bh.consume(p.nextDoc());
		}
	}
}