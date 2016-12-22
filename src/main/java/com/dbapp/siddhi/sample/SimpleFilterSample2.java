/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dbapp.siddhi.sample;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class SimpleFilterSample2 {

    public static void main ( String[] args ) throws InterruptedException {

        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        //from every( e1=TempStream ) -> e2=TempStream[e1.roomNo==roomNo and (e1.temp + 5) <= temp ]
        //within 10 min
        //select e1.roomNo, e1.temp as initialTemp, e2.temp as finalTemp
        //insert into AlertStream;

        String executionPlan = "" +
                "define stream testStream1 ( a string, b string, c string ); " +
                "define stream testStream2 ( d string, e string, f string ); " +
                "" +
                "@info(name = 'query1') " +
                "from every( e1 = testStream1<2:> ) -> e2 = testStream2[ e1.a == e2.d ] " +
                    "within 10 second "+
                "select e1.a, e1.b, e1.c, e2.d, e2.e, e2.f " +
                "insert into outputStream ;";

        //System.out.println( executionPlan );
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime( executionPlan );

        executionPlanRuntime.addCallback( "query1", new QueryCallback() {
            @Override
            public void receive ( long timeStamp, Event[] inEvents, Event[] removeEvents ) {
                EventPrinter.print( timeStamp, inEvents, removeEvents );
            }
        } );

        InputHandler inputHandler1 = executionPlanRuntime.getInputHandler( "testStream1" );
        InputHandler inputHandler2 = executionPlanRuntime.getInputHandler( "testStream2" );

        executionPlanRuntime.start();
        inputHandler1.send( new Object[] { "1" , "2" , "3" } );
        //inputHandler1.send( new Object[] { "4" , "5" , "6" } );

        inputHandler2.send( new Object[] { "7" , "8" , "9" } );
        inputHandler2.send( new Object[] { "10" , "11" , "12" } );

        //inputHandler1.send( new Object[] { "123" , "111" , "222" } );
        inputHandler2.send( new Object[] { "123" , "333" , "444" } );
        inputHandler2.send( new Object[] { "1" , "333" , "444" } );

        Thread.sleep( 500 );

        shutdown( siddhiManager, executionPlanRuntime );
    }

    private static void shutdown ( SiddhiManager siddhiManager, ExecutionPlanRuntime executionPlanRuntime ) {
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
