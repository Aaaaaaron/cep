package edu.zjgsu.siddhi;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Created by AH on 2016/12/21.
 */
public class RelationEval {
    public static void main ( String[] args ) throws InterruptedException {
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        String bruteForceLoginSuccess = "" +
                "define stream rawStream ( catBehavior string, catOutcome string, srcAddress string, deviceCat string, srcUsername string, catObject string, destAddress string, appProtocol string ); " +
                "" +
                "@info(name = 'condition1') " +//catObject要有三种 以srcAddress, srcUsername, destAddress, appProtocol分组的事件个数不少于9个
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'FAIL' and not( srcAddress is null ) ]#window.timeBatch(20 sec) " +
                "select srcAddress, catOutcome, deviceCat, srcUsername, destAddress, appProtocol, distinctcount( catObject ) as distinctMinCount ,count() as groupCount " +
                "group by srcAddress, srcUsername, destAddress, appProtocol " +
                "having groupCount >= 9 and distinctMinCount >=3 " +
                "insert into e1_OutputStream;" +
                "" +
                "@info(name = 'condition2') " +
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'OK' and not( srcAddress is null ) ]#window.timeBatch(20 sec) " +
                "select srcAddress, catOutcome,  deviceCat, srcUsername, destAddress, appProtocol " +
                "insert current events into e2_OutputStream;"
                + "" +
                "@info(name = 'result') " +
                "from every ( e1 = e1_OutputStream ) -> every ( e2 = e2_OutputStream[ e1.srcAddress == srcAddress " +
                                                                         "and e1.deviceCat == deviceCat " +
                                                                         "and e1.srcUsername == srcUsername " +
                                                                         "and e1.destAddress == destAddress " +
                                                                         "and e1.appProtocol == appProtocol ] ) "+
                //"within 1 second " +//每个事件之间的间隔
                "select 'relationEvent' as event, e1.srcAddress, e1.deviceCat, e1.srcUsername, e1.destAddress, e1.appProtocol " +
                "insert into resultOutputStream;"
                ;

        //System.out.println( bruteForceLoginSuccess );
        //Generating runtime
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime( bruteForceLoginSuccess );

        executionPlanRuntime.addCallback( "e1_OutputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        } );

        executionPlanRuntime.addCallback( "e2_OutputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        } );

        executionPlanRuntime.addCallback( "result", new QueryCallback() {
            @Override
            public void receive ( long timeStamp, Event[] inEvents, Event[] removeEvents ) {
                EventPrinter.print( timeStamp, inEvents, removeEvents );
            }
        } );

        InputHandler rawStreamHandler = executionPlanRuntime.getInputHandler( "rawStream" );
        executionPlanRuntime.start();
        //catBehavior, catOutcome, srcAddress, deviceCat, srcUsername, catObject, destAddress, appProtocol ;group by srcAddress, srcUsername, destAddress, appProtocol
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject2" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject3" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" , "deviceCat" , "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );

        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" , "deviceCat" ,
                "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );


        Thread.sleep( 1000 * 200 );
        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }
}
