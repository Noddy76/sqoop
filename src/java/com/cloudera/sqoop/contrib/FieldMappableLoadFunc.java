/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sqoop.contrib;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.cloudera.sqoop.lib.FieldMappable;

/**
 * Pig LoadFunc for accessing Sqoop sequence files. To use generate a sequence
 * file using sqoop and then in pig use this LoadFunc to access that dataset.
 * An example Pig statement would be:
 * <pre>
 * dataset = LOAD 'hdfs://hdfs.server/path/to/sequencefile'
 *   USING FieldMappableLoadFunc
 *   AS (
 *     id,
 *     name
 *   );
 * </pre>
 * <string>Note: The field names must be listed in alphabetical order!</strong>
 */
public class FieldMappableLoadFunc extends LoadFunc {
  private final TupleFactory tupleFactory = TupleFactory.getInstance();

  private SequenceFileRecordReader<Writable, Writable> reader;
  private byte[] types = null;

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() throws IOException {
    return new SequenceFileInputFormat<Writable, Writable>();
  }

  @Override
  public LoadCaster getLoadCaster() throws IOException {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) throws IOException {
    this.reader = (SequenceFileRecordReader<Writable, Writable>) reader;
  }

  @Override
  public Tuple getNext() throws IOException {
    int i;
    try {
      boolean recordRead = reader.nextKeyValue();
      if (!recordRead) {
        return null;
      }

      FieldMappable value = (FieldMappable) reader.getCurrentValue();
      Map<String, Object> fieldMap = value.getFieldMap();

      if (types == null) {
        types = new byte[fieldMap.size()];
      }
      i = 0;
      for (Object o : fieldMap.values()) {
        if (types[i] == DataType.UNKNOWN || types[i] == DataType.NULL) {
          types[i] = inferPigDataType(o);
        }
        i++;
      }

      List<Object> result = new ArrayList<Object>();
      i = 0;
      for (Map.Entry<String, Object> e : fieldMap.entrySet()) {
        Object o = translateToPigDataType(e.getValue(), types[i++]);
        result.add(o);
      }

      return tupleFactory.newTuple(result);
    } catch (InterruptedException cause) {
      throw new IOException(cause);
    }
  }

  protected byte inferPigDataType(Object o) {
    if (o == null) {
      return DataType.NULL;
    }

    Class<?> c = o.getClass();
    if (Integer.class.isAssignableFrom(c)) {
      return DataType.INTEGER;
    } else if (Long.class.isAssignableFrom(c)) {
      return DataType.LONG;
    } else if (Date.class.isAssignableFrom(c)) {
      return DataType.LONG;
    } else if (Float.class.isAssignableFrom(c)) {
      return DataType.FLOAT;
    } else if (Double.class.isAssignableFrom(c)) {
      return DataType.DOUBLE;
    } else if (BigDecimal.class.isAssignableFrom(c)) {
      return DataType.DOUBLE;
    } else if (String.class.isAssignableFrom(c)) {
      return DataType.CHARARRAY;
    } else if (BytesWritable.class.isAssignableFrom(c)) {
      return DataType.BYTEARRAY;
    } else if (List.class.isAssignableFrom(c)) {
      return DataType.TUPLE;
    } else {
      return DataType.ERROR;
    }
  }

  protected Object translateToPigDataType(Object o, byte dataType) {
    switch (dataType) {
    case DataType.NULL:
      return null;
    case DataType.INTEGER:
    case DataType.FLOAT:
    case DataType.CHARARRAY:
      return o;
    case DataType.LONG:
      if (o instanceof Date) {
        return ((Date) o).getTime();
      } else { // o is a Long
        return o;
      }
    case DataType.DOUBLE:
      return ((Number) o).doubleValue();
    case DataType.BYTEARRAY:
      return new DataByteArray(((BytesWritable) o).getBytes());
    case DataType.TUPLE:
      if (o == null) {
        return null;
      } else {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) o;
        return tupleFactory.newTupleNoCopy(list);
      }
    }
    return null;
  }
}
