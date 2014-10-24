/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.flow.tez.Hadoop2TezFlowProcess;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;

public class TezJobWithSlowCompilation
  {

  public static void main( String[] args ) throws IOException
    {
    int numInputPaths = 100;
    Map<String, Tap> sources = new HashMap<String, Tap>( numInputPaths );
    Pipe pipe = null;

    String root = "input";

    for( int i = 0; i < numInputPaths; i++ )
      {
      Tap tap = new Hfs( new TextDelimited( new Fields( "test", "testValue" ), true, "\t" ), root + "/" + i + ".txt" );

      TupleEntryCollector collector = tap.openForWrite( new Hadoop2TezFlowProcess() );

      collector.add( new Tuple( "a", "1" ) );

      collector.close();

      sources.put( tap.getIdentifier(), tap );

      if( pipe == null )
        pipe = new Pipe( tap.getIdentifier() );
      else
        pipe = new Merge( pipe, new Pipe( tap.getIdentifier() ) );
      }

    Properties properties = new Properties();
    properties.setProperty( FlowRuntimeProps.GATHER_PARTITIONS, "1" );
    AppProps.setApplicationJarClass( properties, TezJobWithSlowCompilation.class );
    FlowConnector flowConnector = new Hadoop2TezFlowConnector( properties );

    // create the sink tap
    Tap outTap = new Hfs( new TextDelimited( true, "\t" ), "output" );

    // run the flow
    long millis = System.currentTimeMillis();
    System.out.println( "planning" );
    Flow connect = flowConnector.connect( sources, outTap, pipe );
    System.out.println( "done: " + Util.formatDurationFromMillis( System.currentTimeMillis() - millis ) );

    millis = System.currentTimeMillis();
    System.out.println( "running" );
    connect.complete();
    System.out.println( "done: " + Util.formatDurationFromMillis( System.currentTimeMillis() - millis ) );

    System.out.println( "Output in " + outTap );
    }
  }