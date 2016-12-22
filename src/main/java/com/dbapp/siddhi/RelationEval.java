package com.dbapp.siddhi;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by AH on 2016/12/21.
 */
public class RelationEval {
    public static void main ( String[] args ) throws InterruptedException {
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        String bruteForceLoginSuccess = "" +
                "define stream rawStream ( catBehavior string, catOutcome string, srcAddress string, deviceCat string, srcUsername string, catObject string, destAddress string, appProtocol string ); " +
                //"define stream condition2Stream ( catBehavior string, catOutcome string, srcAddress string, deviceCat string, srcUsername string, catObject string, destAddress string, appProtocol string ); " +
                "" +
                "@info(name = 'condition1') " +
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'FAIL' and not( srcAddress is null ) ]#window.time(1 min) " +
                "select srcAddress, deviceCat, srcUsername, destAddress, appProtocol " +//soc join部分要的字段取出来
                "insert into e1_OutputStream;" +
                "" +
                "@info(name = 'condition2') " +
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'OK' and not( srcAddress is null ) ]#window.time(1 min)  " +
                "select srcAddress, deviceCat, srcUsername, destAddress, appProtocol " +
                "insert into e2_OutputStream;"
                + "" +
                "@info(name = 'result') " +
                "from every ( e1 = e1_OutputStream<9:> ) -> e2 = e2_OutputStream[ e1.srcAddress == srcAddress " +
                                                                         "and e1.deviceCat == deviceCat " +
                                                                         "and e1.srcUsername == srcUsername " +
                                                                         "and e1.destAddress == destAddress " +
                                                                         "and e1.appProtocol == appProtocol ] "+
                "within 10 second " +
                "select 'relationEvent' as event, e1.srcAddress, e1.deviceCat, e1.srcUsername, e1.destAddress, e1.appProtocol " +
                "insert into resultOutputStream;"
                ;

        //System.out.println( bruteForceLoginSuccess );
        //Generating runtime
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime( bruteForceLoginSuccess );

        executionPlanRuntime.addCallback( "resultOutputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        } );

        //executionPlanRuntime.addCallback( "result", new QueryCallback() {
        //    @Override
        //    public void receive ( long timeStamp, Event[] inEvents, Event[] removeEvents ) {
        //        EventPrinter.print( timeStamp, inEvents, removeEvents );
        //    }
        //} );

        InputHandler rawStreamHandler = executionPlanRuntime.getInputHandler( "rawStream" );

        //Starting event processing
        executionPlanRuntime.start();

        //Sending events to Siddhi
        for ( int i = 0 ; i < 9 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,"deviceCat", "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        }
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" ,"deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );

        //Thread.sleep( 1000*70 );

        for ( int i = 0 ; i < 9 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,"deviceCat", "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        }
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" ,"deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        Thread.sleep( 500 );


        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }
}
