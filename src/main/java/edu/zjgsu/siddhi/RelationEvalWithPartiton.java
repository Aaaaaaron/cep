package edu.zjgsu.siddhi;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by AH on 2016/12/21.
 */
public class RelationEvalWithPartiton {
    public static void main ( String[] args ) throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String bruteForceLoginSuccess =
                "@plan:async " +
                "define stream rawStream ( catBehavior string, catOutcome string, srcAddress string, deviceCat string, " +
                        "srcUsername string, catObject string, destAddress string, appProtocol string ); " +
                "" +
                "@info(name = 'query1') from rawStream[ catBehavior == '/Authentication/Verify' and not( srcAddress is null ) ]#window.time(10 sec) " +
                "select srcAddress, catOutcome, deviceCat, srcUsername, destAddress, appProtocol " +
                "insert into LoginStream ; " +
                ""+
                "partition with ( srcUsername of rawStream  ) " +
                "begin " +
                "@info(name = 'result') " +
                "from every ( e1 = LoginStream[ catOutcome == 'FAIL' ]<9:> -> e2 = LoginStream[ catOutcome == 'OK']) " +
                //"within 1 second " +//每个事件之间的间隔
                "select 'relationEvent' as event " +
                "insert into resultOutputStream;" +
                "end ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime( bruteForceLoginSuccess );

        executionPlanRuntime.addCallback( "resultOutputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        } );

        executionPlanRuntime.addCallback( "LoginStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        } );

        InputHandler rawStreamHandler = executionPlanRuntime.getInputHandler( "rawStream" );
        executionPlanRuntime.start();
        //catBehavior, catOutcome, srcAddress, deviceCat, srcUsername, catObject, destAddress, appProtocol ;group by srcAddress, srcUsername, destAddress, appProtocol
        //第一个group
        for ( int i = 0 ; i < 13 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
                    "deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol"} );
        }
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" , "deviceCat" ,
                "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );

        //第二个group
        for ( int i = 0 ; i < 12 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "2.2.2.2" ,
                    "deviceCat" , "srcUsername2" , "catObject" , "destAddress" , "appProtocol"} );
        }
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "2.2.2.2" , "deviceCat" ,
                "srcUsername2" , "catObject" , "destAddress" , "appProtocol" } );

        Thread.sleep( 1000 * 200 );
        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }
}
