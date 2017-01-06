package edu.zjgsu.esper.performance;

import com.espertech.esper.client.*;

/**
 * Created by AH on 2017/1/4.
 */
public class TestMultiStreamPartition {
    public static void main ( String[] args ) {
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
        EPAdministrator admin = epService.getEPAdministrator();
        EPRuntime runtime = epService.getEPRuntime();
        admin.createEPL( "create objectarray schema EventA (a string, b string)" );
        admin.createEPL( "create objectarray schema EventB (c string, d string)" );

        admin.createEPL( "create context PartAB \n" +
                "                context PartA partition by a and b from EventA, " +
                "                context PartB partition by d from EventB " );

        String epl =
                "context PartAB  \n" +
                //"select * from pattern [ every( [2:] (e1 = EventA) until (e2 = EventB(e2.d=e1[1].b)) ) ].win:time(10 sec)";
                //"select * from pattern [ every( (e1 = EventA) -> (e2 = EventB(e2.d=e1.b))) ].win:time(10 sec)";
                "select * from pattern [ every( (e1 = EventA) -> (e2 = EventB)) ].win:time(10 sec)";
        EPStatement state = admin.createEPL( epl );

        state.addListener( ( newEvents, oldEvents ) -> {
            for ( EventBean newEvent : newEvents ) {
                System.out.println( "newEvent.get(  )" );
                //System.out.println( newEvent.get( "a.a" ) );
            }
        } );

        simulationData( runtime );
    }

    private static void simulationData ( EPRuntime runtime ) {
        runtime.sendEvent( new Object[] { "1" , "2" }, "EventA" );
        //runtime.sendEvent( new Object[] { "1" , "3" }, "EventA" );
        //runtime.sendEvent( new Object[] { "2" , "3" }, "EventA" );
        //runtime.sendEvent( new Object[] { "3" , "2" }, "EventA" );

        runtime.sendEvent( new Object[] { "3" , "2" }, "EventB" );
        //runtime.sendEvent( new Object[] { "4" , "3" }, "EventB" );
    }
}
