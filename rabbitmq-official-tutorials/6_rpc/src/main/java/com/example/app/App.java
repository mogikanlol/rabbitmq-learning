package com.example.app;

public class App {

    public static void main(String[] args) throws Exception {
        final String hostName = "localhost";
        final String queueName = "rpc_queue";

        Consumer consumer = new Consumer(
                "C-1",
                hostName,
                queueName
        );
        consumer.start();

        try (FibonacciRpcClient fibonacciRpcClient = new FibonacciRpcClient("P-1", hostName, queueName)) {

            for (int i = 0; i < 10; i++) {
                System.out.println("fib(" + i + ") = " + fibonacciRpcClient.call(String.valueOf(i)));
            }

        }

        consumer.stop();
    }

}
