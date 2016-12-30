package edu.zjgsu.siddhi;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * Created by AH on 2016/12/21.
 */
public class RelationEval2 {
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
                "having groupCount >= 1 and distinctMinCount >=1 " +
                //"output last every 10 sec " +
                //"output last every 80 events " +
                //"output snapshot every 1 sec " +
                "insert into e1_OutputStream;" +

                "" +
                "@info(name = 'condition2') " +
                "from rawStream[ catBehavior == '/Authentication/Verify' and catOutcome == 'OK' and not( srcAddress is null ) ]#window.time(10 sec)  " +
                "select srcAddress, catOutcome, deviceCat, srcUsername, destAddress, appProtocol " +
                "insert into e2_OutputStream;"
                + "" +
                "@info(name = 'result') " +
                "from every ( e1 = e1_OutputStream<9:>  ) -> every ( e2 = e2_OutputStream[ " +
                "e1.srcAddress == srcAddress " +
                "and e1.deviceCat == deviceCat " +
                "and e1.srcUsername == srcUsername " +
                "and e1.destAddress == destAddress " +
                "and e1.appProtocol == appProtocol ] ) " +
                "within 1 second " +
                "select 'relationEvent' as event, e1.srcAddress, e1.deviceCat, e1.srcUsername, e1.destAddress, e1.appProtocol " +
                "insert into resultOutputStream;";

        //System.out.println( bruteForceLoginSuccess );
        //Generating runtime
        ExecutionPlanRuntime executionPlanRuntime =
                siddhiManager.createExecutionPlanRuntime( bruteForceLoginSuccess );

        //executionPlanRuntime.addCallback("resultOutputStream", new StreamCallback() {
        //    //public int eventCount = 0;
        //    //public int timeSpent = 0;
        //    //long startTime = System.currentTimeMillis();
        //    //
        //    //@Override
        //    //public void receive(Event[] events) {
        //    //    for (Event event : events) {
        //    //        eventCount++;
        //    //        System.out.println( eventCount );
        //    //    }
        //    //}
        //        @Override
        //        public void receive ( Event[] events ) {
        //            for ( Event event : events ) {
        //                System.out.println( event.toString() );
        //            }
        //        }
        //   });

        executionPlanRuntime.addCallback( "e1_OutputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        } );
        //
        //executionPlanRuntime.addCallback( "e2_OutputStream", new StreamCallback() {
        //    @Override
        //    public void receive ( Event[] events ) {
        //        for ( Event event : events ) {
        //            System.out.println( event.toString() );
        //        }
        //    }
        //} );

        //executionPlanRuntime.addCallback( "resultOutputStream", new StreamCallback() {
        //    @Override
        //    public void receive ( Event[] events ) {
        //        for ( Event event : events ) {
        //            System.out.println( event.toString() );
        //        }
        //    }
        //}  );

        InputHandler rawStreamHandler = executionPlanRuntime.getInputHandler( "rawStream" );
        executionPlanRuntime.start();

        //rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "2.2.2.2" , "deviceCat" ,
        //        "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        //Thread.sleep( 1000 );

            for ( int j = 0 ; j < 9 ; j++ ) {
                rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
                        "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" } );
                rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" , "deviceCat" ,
                        "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
            }

        for ( int j = 0 ; j < 9 ; j++ ) {
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.2" ,
                    "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" } );
            rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" , "deviceCat" ,
                    "srcUsername" , "catObject" , "destAddress" , "appProtocol" } );
        }

        //第二个group
        //for ( int i = 0 ; i < 9 ; i++ ) {
        //    rawStreamHandler.send( new Object[] { "/Authentication/Verify" , "FAIL" , "2.2.2.2" ,
        //            "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" } );
        //}


        Thread.sleep( 1000 );
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();

    }
}