package edu.zjgsu.siddhi;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by AH on 2016/12/21.
 */
public class RelationEval7 {
    public static void main ( String[] args ) throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String bruteForceLoginSuccess = "" +
                "@plan:async " +
                "define stream rawStream ( catBehavior string, catOutcome string, srcAddress string, deviceCat string, srcUsername string, catObject string, destAddress string, appProtocol string,id string ); " +
                "" +
                "@info(name = 'condition1') " +
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'FAIL' and not( srcAddress is null ) ]#window.time(10 sec) " +
                "select srcAddress, catOutcome, deviceCat, srcUsername, destAddress, appProtocol, distinctcount( catObject ) as distinctMinCount, count() as groupCount, id " +
                "group by srcAddress, srcUsername, destAddress, appProtocol " +
                "output snapshot every 10 sec insert into e1_OutputStream;" +
                "" +
                "@info(name = 'condition2') " +
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'OK' and not( srcAddress is null ) ]#window.timeBatch(10 sec) " +
                "select srcAddress, catOutcome,  deviceCat, srcUsername, destAddress, appProtocol, count() as groupCount  " +
                "group by srcAddress, srcUsername, destAddress, appProtocol " +
                "having groupCount >= 1 " +
                "insert current events into e2_OutputStream;"
                + "" +
                "@info(name = 'result') " +
                "from every ( e1 = e1_OutputStream[ groupCount >= 9 ]<9:> ) -> every ( e2 = e2_OutputStream[ srcAddress == e1.srcAddress " +
                                                                                 "and deviceCat == e1.deviceCat " +
                                                                                 "and srcUsername == e1.srcUsername " +
                                                                                 "and destAddress == e1.destAddress " +
                                                                                 "and appProtocol == e1.appProtocol ]<1> ) " +
                //"within 1 second " +//每个事件之间的间隔
                "select 'relationEvent' as event, e1.srcAddress, e1.deviceCat, e1.srcUsername, e1.destAddress, e1.appProtocol " +
                "insert into resultOutputStream;";

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
        //
        executionPlanRuntime.addCallback( "resultOutputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        }  );

        //executionPlanRuntime.addCallback( "result", new QueryCallback() {
        //    @Override
        //    public void receive ( long timeStamp, Event[] inEvents, Event[] removeEvents ) {
        //        if ( inEvents != null )
        //            for ( Event inEvent : inEvents )
        //                System.out.println( inEvent.toString() );
        //        else
        //            System.out.println( "in events is null" );
        //        if ( removeEvents != null )
        //            for ( Event removeEvent : removeEvents )
        //                System.out.println( removeEvent.toString() );
        //        else
        //            System.out.println( "remove events is null" );
        //    }
        //} );

        InputHandler rawStreamHandler = executionPlanRuntime.getInputHandler( "rawStream" );
        executionPlanRuntime.start();
        //catBehavior, catOutcome, srcAddress, deviceCat, srcUsername, catObject, destAddress, appProtocol ;group by srcAddress, srcUsername, destAddress, appProtocol
        //第一个group
        for ( int i = 0 ; i < 13 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
                    "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" ,i} );
        }
        //第二个group
        for ( int i = 0 ; i < 12 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "2.2.2.2" ,
                    "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" ,i} );
        }
        Thread.sleep( 1000 );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" , "deviceCat" ,
                "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "2.2.2.2" , "deviceCat" ,
                "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );

        Thread.sleep( 1000 * 200 );
        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }
}
