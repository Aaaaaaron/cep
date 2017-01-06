package edu.zjgsu.esper.performance;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by AH on 2017/1/4.
 */
public class RelationEvalByMap {
    public static void main ( String[] args ) {
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
        //epService.initialize();
        Map< String, Object > def = new HashMap<>();
        def.put( "price", int.class );
        epService.getEPAdministrator().getConfiguration().addEventType( "testStream", def );

        String exp = "select price from testStream";
        EPStatement statement = epService.getEPAdministrator().createEPL( exp );

        Map< String, Object > event = new HashMap<>();
        event.put( "price", 60 );

        epService.getEPRuntime().sendEvent( event,"testStream" );

        statement.addListener( ( newEvents, oldEvents ) -> {
            EventBean eventBean = newEvents[ 0 ];
            System.out.println( "avg = " + eventBean.get( "avg(price)" ) );
        } );
    }
}
