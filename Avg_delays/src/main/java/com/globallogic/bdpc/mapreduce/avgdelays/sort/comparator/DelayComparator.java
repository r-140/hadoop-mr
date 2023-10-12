package com.globallogic.bdpc.mapreduce.avgdelays.sort.comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public  class DelayComparator extends WritableComparator {

        protected DelayComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DoubleWritable aVal = (DoubleWritable) a;
            DoubleWritable bVal = (DoubleWritable) b;
            return -1 * aVal.compareTo(bVal);
        }

    }
