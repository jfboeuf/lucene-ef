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
import java.util.BitSet;
import java.util.Random;

import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Assert;
import org.junit.Test;

public class TestEFEncoder {

	@Test
	public void testBasic() throws IOException {
		doTest(new int[] { 2, 3, 5, 7, 11, 13, 24 }, 55);
		int[] values16 = new int[] { 2, 3, 5, 7, 11, 13, 24, 100, 200, 300, 400, 500, 1000, 1200, 1500, 1990 };
		doTest(values16, 5000); // aligned on 8
		doTest(values16, 2000); // 6 low bits per value => 96 bits unaligned on 64 bits
		int[] values = { 2, 9, 18, 20, 34, 35, 36, 37, 41, 42, 43, 46, 54, 56, 57, 64, 65, 66, 78, 80, 87, 89, 94, 104,
				106, 129, 137, 138, 139, 140, 141, 147, 152, 208, 214, 246, 251, 255, 262, 267, 277, 289, 302, 306, 307,
				308, 311, 312, 328, 331, 390, 412, 421, 477, 585, 610, 683, 688, 788, 797, 845, 849, 929, 953, 955, 959,
				967, 981, 982, 983, 984, 985, 986, 987, 988, 989, 990, 991, 998, 1013, 1018, 1020, 1057, 1075, 1107,
				1108, 1109, 1148, 1149, 1256, 1269, 1279, 1304, 1305, 1307, 1313, 1327, 1349, 1350, 1351, 1357, 1365,
				1378, 1397, 1409, 1430, 1431, 1432, 1504, 1506, 1543, 1583, 1601, 1602, 1603, 1625, 1650, 1662, 1673,
				1674, 1675, 1688, 1690, 1692, 1704, 1728, 1738, 1739, 1742, 1745, 1793, 1804, 1808, 1813, 1814, 1815,
				1819, 1820, 1823, 1895, 1905, 1913, 1938, 1945, 1963, 2016, 2017, 2030, 2041, 2044, 2057, 2076, 2080,
				2082, 2088, 2092, 2094, 2097, 2098, 2104, 2106, 2108, 2109, 2110, 2114, 2116, 2117, 2118, 2120, 2122,
				2123, 2132, 2145, 2146, 2147, 2148, 2149, 2164, 2166, 2167, 2173, 2191, 2248, 2252, 2293, 2304, 2306,
				2309, 2315, 2324, 2325, 2333, 2341, 2348, 2361, 2378, 2400, 2407, 2440, 2447, 2448, 2451, 2463, 2466,
				2472, 2489, 2490, 2503, 2505, 2507, 2509, 2510, 2511, 2512, 2514, 2515, 2517, 2519, 2522, 2532, 2545,
				2547, 2551, 2553, 2554, 2558, 2560, 2561, 2562, 2573, 2592, 2598, 2640, 2657, 2662, 2667, 2677, 2679,
				2689, 2692, 2695, 2696, 2702, 2705, 2707, 2709, 2713, 2720, 2722, 2728, 2731, 2762, 2763, 2764, 2765,
				2785, 2827, 2828, 2837, 2856, 2859, 2875, 2896, 2897, 2899, 2914, 2918, 2920, 2942, 2943, 2966, 2983,
				2989, 2996, 3062, 3064, 3065, 3101, 3111, 3123, 3127, 3147, 3150, 3152, 3159, 3163, 3169, 3170, 3182,
				3183, 3184, 3186, 3188, 3189, 3200, 3227, 3241, 3250, 3261, 3262, 3263, 3275, 3296, 3297, 3305, 3358,
				3359, 3360, 3363, 3364, 3382, 3389, 3391, 3392, 3402, 3419, 3430, 3437, 3444, 3455, 3458, 3461, 3484,
				3503, 3514, 3534, 3555, 3556, 3560, 3580, 3593, 3609, 3611, 3619, 3621, 3627, 3638, 3648, 3654, 3660,
				3665, 3689, 3702, 3742, 3768, 3777, 3783, 3789, 3790, 3791, 3792, 3793, 3802, 3803, 3806, 3809, 3829,
				3830, 3831, 3837, 3846, 3848, 3858, 3861, 3868, 3891, 3892, 3896, 3906, 3912, 3918, 3923, 3932, 3934,
				3935, 3945, 3947, 3950, 3954, 3958, 3960, 3963, 3966, 3968, 3970, 3990, 4013, 4028, 4030, 4031, 4038,
				4054, 4055, 4095, 4097, 4098, 4101, 4107, 4114, 4130, 4131, 4132, 4133, 4136, 4137, 4151, 4156, 4157,
				4189, 4192, 4200, 4220, 4222, 4223, 4224, 4237, 4238, 4280, 4281, 4296, 4298, 4299, 4300, 4307, 4308,
				4310, 4365, 4366, 4417, 4418, 4419, 4420, 4421, 4422, 4423, 4432, 4433, 4434, 4435, 4441, 4444, 4450,
				4462, 4463, 4465, 4466, 4468, 4469, 4470, 4471, 4474, 4475, 4476, 4478, 4479, 4496, 4499, 4518, 4519,
				4521, 4526, 4527, 4529, 4530, 4536, 4666, 4686, 4698, 4719, 4777, 4794, 4799, 4800, 4802, 4803, 4804,
				4805, 4810, 4811, 4819, 4832, 4840, 4863, 4864, 4888, 4898, 4916, 4921, 4925, 4929, 4939, 4943, 4945,
				4948, 4957, 4966, 4990, 5013, 5015, 5028, 5156, 5166, 5182, 5188, 5189, 5190, 5205, 5227, 5257, 5259,
				5260, 5261, 5262, 5263, 5269, 5296, 5297, 5298, 5299, 5309, 5320, 5327, 5348, 5352, 5360, 5384, 5388,
				5389, 5415, 5422, 5423, 5424, 5475, 5476, 5477, 5478, 5479, 5506, 5525, 5526, 5534, 5590, 5603, 5653,
				5658, 5685, 5693, 5703, 5710, 5747, 5815, 5816, 5817, 5818, 5819, 5825, 5857, 5861, 5868, 5869, 5877,
				5880, 5932, 5981, 5982, 5983, 5995, 6089, 6103, 6121, 6123, 6133, 6134, 6148, 6150, 6151, 6153, 6154,
				6155, 6156, 6170, 6172, 6174, 6178, 6181, 6184, 6186, 6188, 6192, 6197, 6199, 6205, 6206, 6211, 6217,
				6218, 6220, 6221, 6222, 6224, 6227, 6228, 6231, 6232, 6238, 6241, 6242, 6411, 6428, 6429, 6430, 6440,
				6441, 6445, 6469, 6473, 6481, 6502, 6503, 6504, 6505, 6506, 6507, 6508, 6509, 6510, 6511, 6512, 6532,
				6556, 6557, 6660, 6668, 6670, 6687, 6725, 6727, 6728, 6740, 6741, 6744, 6753, 6756, 6764, 6766, 6791,
				6799, 6809, 6810, 6812, 6824, 6826, 6847, 6848, 6849, 6881, 6884, 6892, 6901, 6938, 6946, 6951, 6953,
				6955, 6960, 6974, 6977, 6978, 6979, 6980, 6981, 6982, 6985, 7001, 7002, 7016, 7018, 7019, 7027, 7043,
				7053, 7058, 7059, 7066, 7088, 7096, 7103, 7104, 7109, 7110, 7115, 7117, 7120, 7122, 7125, 7126, 7140,
				7141, 7142, 7145, 7146, 7147, 7160, 7221, 7252, 7253, 7254, 7255, 7256, 7257, 7258, 7259, 7260, 7266,
				7281, 7284, 7296, 7297, 7298, 7301, 7303, 7322, 7324, 7330, 7391, 7430, 7445, 7464, 7468, 7474, 7478,
				7509, 7510, 7514, 7539, 7540, 7575, 7614, 7677, 7678, 7679, 7680, 7681, 7682, 7692, 7696, 7701, 7712,
				7714, 7732, 7754, 7788, 7789, 7797, 7803, 7844, 7851, 7858, 7865, 7866, 7867, 7868, 7869, 7913, 7918,
				7920, 7921, 7924, 7925, 7926, 7927, 7932, 7937, 7945, 7967, 7974, 7987, 7991, 7992, 7998, 7999, 8003,
				8041, 8042, 8045, 8046, 8086, 8087, 8093, 8094, 8099, 8100, 8101, 8102, 8103, 8105, 8106, 8108, 8110,
				8111, 8112, 8114, 8115, 8116, 8117, 8118, 8171, 8200, 8227, 8248, 8437, 8478, 8508, 8530, 8533, 8536,
				8538, 8550, 8551, 8563, 8567, 8570, 8572, 8576, 8583, 8591, 8592, 8593, 8594, 8595, 8599, 8601, 8605,
				8607, 8610, 8611, 8614, 8615, 8616, 8654, 8659, 8664, 8671, 8672, 8678, 8684, 8687, 8692, 8707, 8716,
				8729, 8730, 8744, 8751, 8765, 8768, 8769, 8771, 8777, 8780, 8812, 8813, 8816, 8818, 8924, 8925, 8926,
				8992, 8996, 8999, 9002, 9007, 9015, 9032, 9040, 9041, 9046, 9050, 9057, 9064, 9071, 9112, 9118, 9119,
				9120, 9134, 9138, 9180, 9246, 9256, 9257, 9258, 9260, 9262, 9266, 9267, 9270, 9271, 9272, 9273, 9275,
				9278, 9279, 9280, 9282, 9316, 9327, 9339, 9340, 9349, 9351, 9370, 9371, 9377, 9383, 9390, 9452, 9453,
				9454, 9455, 9456, 9462, 9482, 9489, 9493, 9549, 9585, 9586, 9587, 9594, 9619, 9685, 9686, 9699, 9700,
				9701, 9704, 9705, 9708, 9759, 9804, 9806, 9819, 9820, 9854, 9855, 9859, 9864, 9866, 9868, 9883, 9886,
				9888, 9893, 9918, 9922, 9924, 9929, 9940, 9942, 9962, 9975, 9977, 9978, 9979, 9986, 9987, 9988, 9989,
				9992, 9993, 9994, 9995, 9997 };
		doTest(values, 10000);
		values = new int[] { 140, 319, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474, 475,
				476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496,
				497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517,
				518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538,
				539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559,
				560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580,
				581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 600, 601,
				602, 603, 604, 605, 810, 850, 856, 857, 863, 864, 865, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016,
				1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033,
				1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050,
				1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063, 1064, 1065, 1066, 1067,
				1068, 1069, 1070, 1071, 1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079, 1080, 1081, 1082, 1083, 1084,
				1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1093, 1094, 1095, 1096, 1097, 1098, 1099, 1100, 1101,
				1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1112, 1113, 1114, 1365, 1895, 1980, 2062,
				2063, 2064, 2065, 2066, 2067, 2068, 2098, 2099, 2432, 2442, 2498, 2520, 2862, 2863, 2864, 2865, 2866,
				2867, 2868, 2972, 2973, 2974, 2975, 2976, 2977, 2978, 2979, 2980, 2981, 2982, 2983, 2984, 2985, 2986,
				2987, 2988, 2989, 2990, 2991, 2992, 2993, 2994, 2995, 2996, 2997, 2998, 2999, 3000, 3001, 3002, 3003,
				3004, 3005, 3006, 3007, 3008, 3009, 3068, 3069, 3070, 3071, 3072, 3073, 3074, 3289, 3290, 3291, 3292,
				3293, 3294, 3295, 3296, 3297, 3298, 3299, 3300, 3301, 3302, 3303, 3304, 3305, 3306, 3307, 3308, 3309,
				3310, 3311, 3312, 3313, 3478, 3485, 3486, 3488, 3671, 3791, 4077, 4078, 4079, 4080, 4081, 4082, 4083,
				4084, 4085, 4086, 4087, 4088, 4089, 4090, 4091, 4092, 4093, 4094, 4095, 4096, 4097, 4098, 4099, 4194,
				4212, 4431, 4432, 4433, 4434, 4435, 4436, 4437, 4438, 4439, 4440, 4441, 4442, 4443, 4541, 4542, 4672,
				4719, 4722, 4723, 4724, 4725, 4726, 4727, 4728, 4729, 4730, 4731, 4732, 4733, 4734, 4735, 4736, 4737,
				4738, 4739, 4740, 4921, 4940, 4974, 5209, 5320, 5810, 5811, 6658, 6932, 7087, 7393, 7394, 7395, 7396,
				7397, 7398, 7679, 7788, 7797, 7798, 7803, 7991, 8045, 8051, 8052, 8053, 8054, 8055, 8056, 8057, 8058,
				8059, 8060, 8061, 8062, 8063, 8064, 8065, 8066, 8067, 8068, 8069, 8070, 8071, 8072, 8073, 8074, 8075,
				8076, 8077, 8078, 8079, 8080, 8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088, 8216, 8217, 8218, 8219,
				8220, 8221, 8222, 8223, 8224, 8225, 8226, 8369, 8370, 8371, 8372, 8373, 8374, 8375, 8376, 8377, 8378,
				8379, 8380, 8381, 8382, 8383, 8384, 8385, 8386, 8387, 8388, 8389, 8390, 8391, 8392, 8393, 8394, 8395,
				8396, 8397, 8398, 8399, 8434, 8605, 8606, 8607, 8608, 8609, 8610, 8611, 8612, 8613, 8614, 8615, 8616,
				8617, 8682, 8683, 8684, 8685, 8686, 8687, 8688, 8689, 8690, 8691, 8692, 8693, 8694, 8695, 8696, 8697,
				8698, 8699, 8700, 8701, 8702, 8703, 8704, 8705, 8706, 8707, 8708, 8709, 8710, 8711, 8712, 8713, 8714,
				8715, 8716, 8717, 8718, 8719, 8720, 8721, 8722, 8723, 8724, 8725, 8726, 8727, 8728, 9177, 9178, 9179,
				9180, 9181, 9279, 9584, 9637, 9638, 9639, 9640, 9641, 9642, 9643, 9644, 9645, 9646, 9647, 9648, 9649,
				9650, 9651, 9652, 9653, 9654, 9655, 9656, 9657, 9658, 9659, 9660, 9661, 9662, 9663, 9664, 9665, 9666,
				9667, 9668, 9669, 9670, 9671, 9672, 9673, 9674, 9675, 9676, 9677, 9678, 9679, 9680, 9681, 9682, 9683,
				9684, 9685, 9686, 9687, 9688, 9689, 9690, 9691, 9692, 9693, 9694, 9695, 9696, 9697, 9698, 9699, 9700,
				9701, 9702, 9804 };
		doTest(values, 10000);
		values = new int[] { 1, 48, 92, 94, 136 };
		doTest(values, 200);
	}

	private static void doTest(int[] values, int max) throws IOException {
		ByteBuffersDataOutput bbdo = new ByteBuffersDataOutput();
		try (IndexOutput out = new ByteBuffersIndexOutput(bbdo, "descr", "name")) {
			EFEncoder efEncode = new EFEncoder(out, max, 1 << 29);
			for (int i : values) {
				efEncode.addDoc(i, 1);
			}
			efEncode.finish();
		}
		ByteBuffersIndexInput ixIn = new ByteBuffersIndexInput(bbdo.toDataInput(), "desc");
		IntBlockTermState termState = new IntBlockTermState();
		termState.docFreq = values.length;
		EFPostingsEnum postings = new EFPostingsEnum(ixIn, max);
		postings.reset(termState, 0);
		for (int i = 0; i < values.length; i++) {
			int v1 = values[i];
			int v2 = postings.nextDoc();
			Assert.assertEquals("failure at index " + i, v1, v2);
		}
		Assert.assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
		postings.reset(termState, 0);
		int ix = values.length >> 1;
		Assert.assertEquals(values[ix + 1], postings.advance(values[ix] + 1));
	}

	private static int maxDoc = 1_000_000;

	@Test
	public void testSize() throws IOException {
		Random random = new Random();
		ByteBuffersDataOutput bbdo = new ByteBuffersDataOutput();
		BitSet bs = new BitSet(maxDoc);
		for (int i = 0; i < 50_000; i++) {
			int did = 300000 + random.nextInt(200_000);
			bs.set(did);
		}
		try (IndexOutput out = new ByteBuffersIndexOutput(bbdo, "descr", "name")) {
			for (int partSize : new int[] { 64, 128, 256, 512, 1 << 26 })
				encode(partSize, bs, out);
		}
	}

	private void encode(int partSize, BitSet bs, IndexOutput out) throws IOException {
		EFEncoder efEncode = new EFEncoder(out, maxDoc, partSize);
		long fp = out.getFilePointer();
		efEncode.startTerm();
		for (int did = bs.nextSetBit(0); did != -1; did = bs.nextSetBit(did + 1)) {
			efEncode.addDoc(did, 1);
		}
		efEncode.finish();
		System.err.println("partSize=" + partSize + " size=" + (out.getFilePointer() - fp));
	}
}
