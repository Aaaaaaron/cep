package edu.zjgsu.siddhi;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by AH on 2016/12/21.
 */
public class RelationEval10 {
    public static void main ( String[] args ) throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String bruteForceLoginSuccess = "" +
                "@plan:async " +
                "define stream rawStream ( catBehavior string, catOutcome string" +
                ", srcAddress string, deviceCat string, srcUsername string" +
                ", catObject string, destAddress string, appProtocol string ); " +
                "" +
                "@info(name = 'condition1') " +
                //catObject要有三种 以srcAddress, srcUsername, destAddress, appProtocol分组的事件个数不少于9个
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'FAIL' and not( srcAddress is null ) ] " +
                "select srcAddress, catOutcome, deviceCat, srcUsername, destAddress, appProtocol, distinctcount( catObject ) as distinctMinCount, count() as groupCount " +
                "group by srcAddress, srcUsername, destAddress, appProtocol " +
                //"having groupCount >= 9 and distinctMinCount >=1 " +
                "insert into e1_OutputStream;" +
                "" +
                "@info(name = 'condition2') " +
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'OK' and not( srcAddress is null ) ] " +
                "select srcAddress, catOutcome,  deviceCat, srcUsername, destAddress, appProtocol, count() as groupCount  " +
                "group by srcAddress, srcUsername, destAddress, appProtocol " +
                //"having groupCount >= 1 " +
                "insert current events into e2_OutputStream;"
                + "" +
                "@info(name = 'result') " +
                "from e1_OutputStream[ groupCount >= 9 and distinctMinCount >=1 ]#window.timeBatch(10 sec) as e1 " +
                "join e2_OutputStream#window.timeBatch(10 sec) as e2 " +
                "on e1.srcAddress == e2.srcAddress " +
                "and e1.deviceCat == e2.deviceCat " +
                "and e1.srcUsername == e2.srcUsername " +
                "and e1.destAddress == e2.destAddress " +
                "and e1.appProtocol == e2.appProtocol " +
                //"within 1 second " +//每个事件之间的间隔
                "select 'relationEvent' as event, e1.srcAddress, e1.deviceCat, e1.srcUsername, e1.destAddress, e1.appProtocol " +
                "insert into resultOutputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime( bruteForceLoginSuccess );

        //executionPlanRuntime.addCallback( "e1_OutputStream", new StreamCallback() {
        //    @Override
        //    public void receive ( Event[] events ) {
        //        for ( Event event : events ) {
        //            System.out.println( event.toString() );
        //        }
        //    }
        //} );
        //
        //executionPlanRuntime.addCallback( "e2_OutputStream", new StreamCallback() {
        //    @Override
        //    public void receive ( Event[] events ) {
        //        for ( Event event : events ) {
        //            System.out.println( event.toString() );
        //        }
        //    }
        //} );

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
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" , "deviceCat" ,
                "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "2.2.2.2" , "deviceCat" ,
                "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );

        //第一个group
        for ( int i = 0 ; i < 9 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
                    "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" } );
        }
        //第二个group
        for ( int i = 0 ; i < 9 ; i++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "2.2.2.2" ,
                    "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" } );
        }

        Thread.sleep( 1000 * 20 );
        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }
}
