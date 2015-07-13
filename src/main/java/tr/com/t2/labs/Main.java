package tr.com.t2.labs;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import tr.com.t2.labs.concurrent.FastBlockingExecutor;

import com.google.common.util.concurrent.RateLimiter;

public class Main {

    public static void main(String[] args) throws Exception {
        String hostName = args[0];
        int port = Integer.parseInt(args[1]);
        int rateLimiterVal = Integer.parseInt(args[2]);
        int queueSize = Integer.parseInt(args[3]);
        int threadCount = Integer.parseInt(args[4]);
        String httpProtocol = "http";

        CloseableHttpClient client = createHttpClient1(hostName, port, threadCount);

        Executor executor = new FastBlockingExecutor("http client", queueSize, threadCount);

        AtomicLong counter = new AtomicLong(0);
        new CounterPrintThread(counter).start();

        HttpGet get = new HttpGet(httpProtocol + "://" + hostName + ":" + port);

        RateLimiter rateLimiter = RateLimiter.create(rateLimiterVal);
        while (true) {
            executor.execute(new MultiHttpClientConnRunnable(client, get, counter, rateLimiter));
        }
    }

    private static CloseableHttpClient createHttpClient1(String hostName, int port, int threadCount) throws Exception {
        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setBufferSize(8 * 1024)
                .setFragmentSizeHint(8 * 1024)
                .build();
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setDefaultConnectionConfig(connectionConfig);
        connManager.setMaxTotal(Math.max(100, threadCount));
        connManager.setDefaultMaxPerRoute(Math.max(100, threadCount));
        HttpHost host = new HttpHost(hostName, port);
        connManager.setMaxPerRoute(new HttpRoute(host), Math.max(100, threadCount));
        return HttpClients.createMinimal(connManager);
    }

    public static class CounterPrintThread extends Thread {
        private AtomicLong counter;

        public CounterPrintThread(AtomicLong counter) {
            this.counter = counter;
        }

        public void run() {
            long value = counter.get();
            try {
                while (true) {
                    long t1 = System.currentTimeMillis();
                    Thread.sleep(1000);

                    long newValue = counter.get();
                    long diff = newValue - value;

                    long t2 = System.currentTimeMillis();

                    int messagePerSecond = (int) (((double) diff) / ((t2 - t1) / 1000.0));
                    System.out.println(messagePerSecond + " req/sec");
                    value = newValue;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static class MultiHttpClientConnRunnable implements Runnable {
        private CloseableHttpClient client;
        private HttpGet get;
        private AtomicLong counter;
        private RateLimiter rateLimiter;

        public MultiHttpClientConnRunnable(CloseableHttpClient client, HttpGet get, AtomicLong counter, RateLimiter rateLimiter) {
            this.client = client;
            this.get = get;
            this.counter = counter;
            this.rateLimiter = rateLimiter;
        }

        @Override
        public void run() {
            rateLimiter.acquire();

            try {
                HttpResponse response = client.execute(get);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    System.out.println(statusCode);
                }
                EntityUtils.consume(response.getEntity());
                counter.incrementAndGet();
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }
    }
}
