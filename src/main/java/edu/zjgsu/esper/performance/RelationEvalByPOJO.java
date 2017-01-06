package edu.zjgsu.esper.performance;

import com.espertech.esper.client.*;
import edu.zjgsu.esper.performance.pojo.RawEvent;

/**
 * Created by AH on 2017/1/4.
 */
public class RelationEvalByPOJO {
    public static void main ( String[] args ) {
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
        EPAdministrator admin = epService.getEPAdministrator();
        EPRuntime runtime = epService.getEPRuntime();

        //rawStream换成pattern
        String epl =
                //pattern [pattern_expression] [.view_spec] [.view_spec] [...]
                "select a.catBehavior from pattern [a =" + RawEvent.class.getName() + " (catOutcome = 'FAIL')]";
                //"select b.catBehavior from pattern [ every( [9] (a = rawStream(catOutcome = 'FAIL')) -> (b = rawStream(catOutcome = 'OK')) ) ]";
                //"select a.catBehavior from pattern [ every( [9] (a = rawStream(catOutcome = 'FAIL')) -> (b = rawStream(catOutcome = 'OK')) ) ].std:groupwin( srcUsername ).win:time( 30 sec ) group by srcUsername";
                //"select * from rawStream(catOutcome = 'FAIL')";
        //.std:groupwin( srcUsername ).win:time( 30 sec ) group by srcUsername
        EPStatement state = admin.createEPL( epl );

        state.addListener( ( newEvents, oldEvents ) -> {
            //System.out.println(  "catOutcome"  );
            System.out.println( newEvents[ 0 ].get( "a.catBehavior" ) );
        } );

        simulationData( runtime );
    }

    private static void simulationData ( EPRuntime runtime ) {
        //for ( int i = 0 ; i < 9 ; i++ ) {
        //    runtime.sendEvent( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
        //            "deviceCat" , "srcUsername" + i , "catObject1" , "destAddress" ,
        //            "appProtocol" }, "rawStream" );
        //}
        //runtime.sendEvent( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" ,
        //        "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" }, "rawStream" );
        RawEvent event = new RawEvent( "/Authentication/Verify","FAIL","1.1.1.1","deviceCat","srcUsername","catObject","destAddress","appProtocol" );
        runtime.sendEvent( event );
    }
}
