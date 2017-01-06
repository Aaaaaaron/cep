package edu.zjgsu.esper.performance;

import com.espertech.esper.client.*;

/**
 * Created by AH on 2017/1/4.
 */
public class TestGroupWin {
    public static void main ( String[] args ) {
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
        EPAdministrator admin = epService.getEPAdministrator();
        EPRuntime runtime = epService.getEPRuntime();
        admin.createEPL( "create objectarray schema StockTickEvent (symbol string, price int)" );
        //rawStream换成pattern
        String epl =
                "select symbol, count(*), sum(price) from StockTickEvent.std:groupwin(symbol).win:time(15) group by symbol";
        EPStatement state = admin.createEPL( epl );

        state.addListener( ( newEvents, oldEvents ) -> {
            for ( EventBean newEvent : newEvents ) {
                System.out.println( newEvent.get( "symbol" ) );
                System.out.println( newEvent.get( "sum(price)" ) );
                System.out.println( newEvent.get( "count(*)" ) );
                System.out.println();
            }
        } );

        simulationData( runtime );
    }

    private static void simulationData ( EPRuntime runtime ) {
        runtime.sendEvent( new Object[] { "1" , 1 }, "StockTickEvent" );
        runtime.sendEvent( new Object[] { "2" , 2 }, "StockTickEvent" );
        runtime.sendEvent( new Object[] { "2" , 2 }, "StockTickEvent" );
        runtime.sendEvent( new Object[] { "3" , 3 }, "StockTickEvent" );
        runtime.sendEvent( new Object[] { "3" , 3 }, "StockTickEvent" );
        runtime.sendEvent( new Object[] { "3" , 3 }, "StockTickEvent" );
        //for ( int i = 0 ; i < 9 ; i++ ) {
        //    runtime.sendEvent( new Object[] { "/Authentication/Verify" , "FAIL" , "1.1.1.1" ,
        //            "deviceCat" , "srcUsername" + i , "catObject1" , "destAddress" ,
        //            "appProtocol" }, "rawStream" );
        //}
        //runtime.sendEvent( new Object[] { "/Authentication/Verify" , "OK" , "1.1.1.1" ,
        //        "deviceCat" , "srcUsername" , "catObject1" , "destAddress" , "appProtocol" }, "rawStream" );
    }
}
