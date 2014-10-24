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

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.util.Util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LocalJobWithSlowCompilation
  {

  public static void main( String[] args ) throws IOException
    {
    int numInputPaths = 100;
    Map<String, Tap> sources = new HashMap<String, Tap>( numInputPaths );
    Pipe pipe = null;
    for( int i = 0; i < numInputPaths; i++ )
      {
      // create simple file
      File tempFile = File.createTempFile( "test", ".txt" );
      tempFile.deleteOnExit();
      PrintWriter writer = new PrintWriter( tempFile );
      try
        {
        writer.println( "test" );
        writer.println( "testValue" );
        }
      finally
        {
        writer.close();
        }

      // add tap
      sources.put( tempFile.getAbsolutePath(), new FileTap( new TextDelimited( true, "\t" ), tempFile.getAbsolutePath() ) );

      if( pipe == null )
        pipe = new Pipe( tempFile.getAbsolutePath() );
      else
        pipe = new Merge( pipe, new Pipe( tempFile.getAbsolutePath() ) );
      }

    File outFile = File.createTempFile( "test", ".txt" );

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, LocalJobWithSlowCompilation.class );
    LocalFlowConnector flowConnector = new LocalFlowConnector( properties );

    // create the sink tap
    Tap outTap = new FileTap( new TextDelimited( true, "\t" ), outFile.getAbsolutePath() );

    // run the flow
    long millis = System.currentTimeMillis();
    System.out.println( "planning" );
    Flow connect = flowConnector.connect( sources, outTap, pipe );
    System.out.println( "done: " + Util.formatDurationFromMillis( System.currentTimeMillis() - millis ) );

    millis = System.currentTimeMillis();
    System.out.println( "running" );
    connect.complete();
    System.out.println( "done: " + Util.formatDurationFromMillis( System.currentTimeMillis() - millis ) );

    System.out.println( "Output in " + outFile );
    }
  }