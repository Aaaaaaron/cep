package com.dbapp.siddhi;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Created by AH on 2016/12/21.
 */
public class RelationEval {
    public static void main ( String[] args ) throws InterruptedException {
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        String bruteForceLoginSuccess = "" +
                "define stream condition1Stream ( catBehavior string, catOutcome string, srcAddress string, deviceCat string, srcUsername string, catObject string, destAddress string, appProtocol string ); " +
                //"define stream condition2Stream ( catBehavior string, catOutcome string, srcAddress string, deviceCat string, srcUsername string, catObject string, destAddress string, appProtocol string ); " +
                "" +
                "@info(name = 'condition1') " +//condition部分
                "from condition1Stream[ catBehavior == '/Authentication/Verify' and catOutcome=='FAIL' and srcAddress != null ] " +
                "select srcAddress, deviceCat, srcUsername, destAddress, appProtocol " +//soc join部分要的字段取出来
                "insert into e1_OutputStream;" +
                "" +
                "@info(name = 'condition2') " +//condition部分
                "from condition1Stream[ catBehavior == '/Authentication/Verify' and catOutcome=='OK' and srcAddress != null ] " +
                "select srcAddress, deviceCat, srcUsername, destAddress, appProtocol " +//soc join部分要的字段取出来
                "insert into e2_OutputStream;"+
                "" +
                "from every ( e1 = e1_OutputStream ) -> e2 = e2_OutputStream[ e1.srcAddress == srcAddress " +
                                                                         "and e1.deviceCat == deviceCat " +
                                                                         "and e1.srcUsername == srcUsername " +
                                                                         "and e1.destAddress == destAddress " +
                                                                         "and e1.appProtocol == appProtocol ] "+
                "within 10 min "


                ;

        /*
from every( e1=TempStream ) -> e2=TempStream[e1.roomNo==roomNo and (e1.temp + 5) <= temp ]
    within 10 min
select e1.roomNo, e1.temp as initialTemp, e2.temp as finalTemp
insert into AlertStream;
         */

        //System.out.println( executionPlan );
        //Generating runtime
        ExecutionPlanRuntime executionPlanRuntime =
                siddhiManager.createExecutionPlanRuntime( bruteForceLoginSuccess );

        //Adding callback to retrieve output events from query
        executionPlanRuntime.addCallback( "condition", new QueryCallback() {
            @Override
            public void receive ( long timeStamp, Event[] inEvents, Event[] removeEvents ) {
                EventPrinter.print( timeStamp, inEvents, removeEvents );
            }
        } );

        //Retrieving InputHandler to push events into Siddhi
        InputHandler inputHandler = executionPlanRuntime.getInputHandler( "condition1Stream" );

        //Starting event processing
        executionPlanRuntime.start();

        //Sending events to Siddhi
        inputHandler.send( new Object[] { "/Application" , 5 } );
        //inputHandler.send( new Object[] { "/Application"} );
        Thread.sleep( 500 );

        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }
}
