package edu.zjgsu.siddhi;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by AH on 2016/12/21.
 */
public class RelationEval3 {
    public static void main ( String[] args ) throws InterruptedException {
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        String bruteForceLoginSuccess = "" +
                "define stream rawStream ( catBehavior string, catOutcome string, " +
                "srcAddress string, deviceCat string, srcUsername string, " +
                "catObject string, destAddress string, appProtocol string ); " +
                "" +
                "@info(name = 'condition1') " +
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'FAIL' and not( srcAddress is null ) ]#window.time(10 sec) " +
                "select srcAddress, catOutcome, deviceCat, srcUsername, destAddress, appProtocol, distinctcount( catObject ) as distinctMinCount, count() as groupCount " +
                "group by srcAddress, srcUsername, destAddress, appProtocol " +
                "insert into e1_OutputStream;" +
                "" +

                "@info(name = 'result') " +
                //"from  ( e1 = e1_OutputStream[ (e1[last].groupCount) >= 1 ] ) " +
                "from  ( e1 = e1_OutputStream ) " +
                "select 'relationEvent' as event, e1.srcAddress, e1.deviceCat, e1.srcUsername, e1.destAddress, e1.appProtocol, e1[last].groupCount " +
                "insert into resultOutputStream;";

        //System.out.println( bruteForceLoginSuccess );
        //Generating runtime
        ExecutionPlanRuntime executionPlanRuntime =
                siddhiManager.createExecutionPlanRuntime( bruteForceLoginSuccess );

        executionPlanRuntime.addCallback( "e1_OutputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        } );

        executionPlanRuntime.addCallback( "resultOutputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        }  );

        InputHandler rawStreamHandler = executionPlanRuntime.getInputHandler( "rawStream" );
        executionPlanRuntime.start();

        for ( int i = 0 ; i < 9 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
                    "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" } );
        }
        //第二个group
        for ( int i = 0 ; i < 9 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "2.2.2.2" ,
                    "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" } );
        }
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" , "deviceCat" ,
                "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "2.2.2.2" , "deviceCat" ,
                "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );

        Thread.sleep( 1000 );
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();

    }
}