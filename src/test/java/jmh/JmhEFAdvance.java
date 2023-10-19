package jmh;

import java.io.IOException;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class JmhEFAdvance extends JmhEFPostingsBase {
	@Benchmark
	public void testAdvanceNext(Blackhole bh) throws IOException {
		postings.reset(termState, 0);
		for (int i = 0; i < 10_000; i++) {
			bh.consume(postings.nextDoc());
		}
	}
}
