import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class DistinctcountAttributeAggregatorTestCase {
    private volatile int count;

    @Before
    public void init() {
        count = 0;
    }

    @Test
    public void DistinctCountTest() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (eventId string, userID string, pageID string); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#window.time(5 sec) " +
                "select userID, pageID, distinctcount(pageID) as distinctPages " +
                "group by userID " +
                //"having distinctPages > 3 " +
                "insert into outputStream; ";

        ExecutionPlanRuntime
                executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        //executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
        //    @Override
        //    public void receive(org.wso2.siddhi.core.event.Event[] events) {
        //        for (org.wso2.siddhi.core.event.Event event : events) {
        //            Assert.assertEquals("User ID", "USER_1", event.getData(0));
        //            Assert.assertEquals("Page ID", "WEB_PAGE_4", event.getData(1));
        //            Assert.assertEquals("Distinct Pages", 4L, event.getData(2));
        //            count++;
        //        }
        //    }
        //});

        executionPlanRuntime.addCallback( "outputStream", new StreamCallback() {
            @Override
            public void receive ( Event[] events ) {
                for ( Event event : events ) {
                    System.out.println( event.toString() );
                }
            }
        } );

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"E001","USER_1","WEB_PAGE_1"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E002","USER_2","WEB_PAGE_1"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E003","USER_1","WEB_PAGE_2"}); // 1st Event in window
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E004","USER_2","WEB_PAGE_2"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E005","USER_1","WEB_PAGE_3"}); // 2nd Event in window
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E006","USER_1","WEB_PAGE_1"}); // 3rd Event in window
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E007","USER_1","WEB_PAGE_4"}); // 4th Event in window
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E008","USER_2","WEB_PAGE_2"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E009","USER_1","WEB_PAGE_1"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"E010","USER_2","WEB_PAGE_1"});

        Thread.sleep(2000);
        executionPlanRuntime.shutdown();
        Assert.assertEquals("Event count", 1, count);
    }
}