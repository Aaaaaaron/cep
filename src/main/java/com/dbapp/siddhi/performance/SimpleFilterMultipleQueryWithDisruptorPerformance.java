/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dbapp.siddhi.performance;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class SimpleFilterMultipleQueryWithDisruptorPerformance {
    private static int count = 0;
    private static volatile long start = System.currentTimeMillis();

    public static void main(String[] args) throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@plan:parallel" +
                " " +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "define stream cseEventStream2 (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[70 > price] " +
                "select * " +
                "insert into outputStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from cseEventStream[volume > 90] " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            private long chunk = 0;
            private long prevCount = 0;

            @Override
            public void receive(Event[] events) {
                count += events.length;
                long currentChunk = count / 2000000;
                if (currentChunk != chunk) {
                    long end = System.currentTimeMillis();
                    double tp = ((count - prevCount) * 1000.0 / (end - start));
                    System.out.println("Throughput = " + tp + " Event/sec");
                    start = end;
                    chunk = currentChunk;
                    prevCount = count;
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
//        InputHandler inputHandler2 = executionPlanRuntime.getInputHandler("cseEventStream2");
        executionPlanRuntime.start();
        while (true) {
            inputHandler.send(new Object[]{"WSO2", 55.6f, 100});
            inputHandler.send(new Object[]{"IBM", 75.6f, 100});
            inputHandler.send(new Object[]{"WSO2", 100f, 80});
            inputHandler.send(new Object[]{"IBM", 75.6f, 100});
            inputHandler.send(new Object[]{"WSO2", 55.6f, 100});
            inputHandler.send(new Object[]{"IBM", 75.6f, 100});
            inputHandler.send(new Object[]{"WSO2", 100f, 80});
            inputHandler.send(new Object[]{"IBM", 75.6f, 100});
        }
    }
}
