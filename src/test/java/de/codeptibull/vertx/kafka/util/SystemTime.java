package de.codeptibull.vertx.kafka.util;

import kafka.utils.Time;

/**
 * http://pannoniancoder.blogspot.de/2014/08/embedded-kafka-and-zookeeper-for-unit.html
 * https://gist.github.com/vmarcinko/e4e58910bcb77dac16e9
 */
class SystemTime implements Time {
    public long milliseconds() {
        return System.currentTimeMillis();
    }
 
    public long nanoseconds() {
        return System.nanoTime();
    }
 
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}