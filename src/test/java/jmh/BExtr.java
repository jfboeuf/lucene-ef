package jmh;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

public class BExtr {
	private static final long value = 0xc1f891118c333c4el;
	private static final int bCount = 3;
	private static final long mask = (1 << bCount) - 1;

	public static void main(String[] args) throws RunnerException {
		for (int i = 0; i < 974; i++)
			System.err.println(i + " \t " + 3 * i + " \t " + Long.toBinaryString(7l << (3 * i)));
	}

	@Benchmark
	public long canonical() {
		long l = 0;
		for (int pos = 0; pos < 22; pos++) {
			int bitPos = (bCount * pos) & 63;
			l += (value >> bitPos) & ((1 << bCount) - 1);
		}
		return l;
	}

	@Benchmark
	public long withPresetBitSet() {
		long l = 0;
		for (int pos = 0; pos < 22; pos++) {
			int bitPos = (bCount * pos) & 63;
			l += (value >>> bitPos) & mask;
		}
		return l;
	}
}