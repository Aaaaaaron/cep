package edu.zjgsu.esper.performance;

import com.espertech.esper.client.*;

/**
 * Created by AH on 2017/1/4.
 */
public class BruteForceLoginDetectSuccess {
    public static void main ( String[] args ) {
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
        EPAdministrator admin = epService.getEPAdministrator();
        EPRuntime runtime = epService.getEPRuntime();
        admin.createEPL(
                "create objectarray schema rawStream (catBehavior string, catOutcome string, srcAddress string," +
                        " deviceCat string, srcUsername string, catObject string, destAddress string, appProtocol string)" );

        admin.createEPL( "create context Login partition by srcUsername " +
                "and srcAddress " +
                "and deviceCat " +
                "and destAddress " +
                "and appProtocol from rawStream(catBehavior = '/Authentication/Verify')" );

        String epl = "context Login \n" +
                "select * from pattern [ every( [9:] (a = rawStream(catOutcome = 'FAIL')) until (b = rawStream(catOutcome = 'OK')) ) ].win:time(10 sec)";
        EPStatement state = admin.createEPL( epl );

        state.addListener( ( newEvents, oldEvents ) -> {
            for ( EventBean newEvent : newEvents ) {
                System.out.println( newEvent.get( "b.catBehavior" ) );
            }
        } );

        simulationData( runtime );
    }

    private static void simulationData ( EPRuntime runtime ) {

        runtime.sendEvent( new Object[] { "/Authentication/Verify" , "OK" , "2.2.2.2" ,
                "deviceCat" , "srcUsername1" , "catObject1" , "destAddress" , "appProtocol" }, "rawStream" );
        for ( int i = 0 ; i < 9 ; i++ ) {
        runtime.sendEvent( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
                "deviceCat" , "srcUsername2" , "catObject1" , "destAddress" ,
                "appProtocol" }, "rawStream" );
        }

        for ( int i = 0 ; i < 9 ; i++ ) {
            runtime.sendEvent( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
                    "deviceCat" , "srcUsername1" , "catObject1" , "destAddress" ,
                    "appProtocol" }, "rawStream" );
        }

        for ( int i = 0 ; i < 9 ; i++ ) {
            runtime.sendEvent( new Object[] { "/Authentication/Verify" , "FAIL" , "2.2.2.2" ,
                    "deviceCat" , "srcUsername1" , "catObject1" , "destAddress" ,
                    "appProtocol" }, "rawStream" );
        }

        runtime.sendEvent( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" ,
                "deviceCat" , "srcUsername1" , "catObject1" , "destAddress" , "appProtocol","3" }, "rawStream" );

        runtime.sendEvent( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" ,
                "deviceCat" , "srcUsername2" , "catObject1" , "destAddress" , "appProtocol" }, "rawStream" );
    }
}
