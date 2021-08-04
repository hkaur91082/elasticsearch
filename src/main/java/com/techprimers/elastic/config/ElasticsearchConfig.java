package com.techprimers.elastic.config;




import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;
 
/**
 * ElasticsearchConfig
 *
 * @author lxx
 * @date 2019/11/11
 */
@Configuration
@Slf4j
public class ElasticsearchConfig implements FactoryBean<RestHighLevelClient>, InitializingBean, DisposableBean {
 
   /* @Value("${mytest.elasticsearch.rest.uri}")
    private String uri;*/
 
    // Java Low Level REST Client (If you want to use a higher version client, you must rely on the lower version client)
    private RestClient client;
         // Java High Level REST Client (high version client)
    private RestHighLevelClient restHighLevelClient;
 
         // Initialize client
    protected void initClient() {
    	
        //RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
                
    	RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http")).setRequestConfigCallback(
    	        new RestClientBuilder.RequestConfigCallback() {
    	            @Override
    	            public RequestConfig.Builder customizeRequestConfig(
    	                    RequestConfig.Builder requestConfigBuilder) {
    	                return requestConfigBuilder
    	                    .setConnectTimeout(5000)
    	                    .setSocketTimeout(60000);
    	            }
    	        });
        restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        client = restHighLevelClient.getLowLevelClient();
 

    }
 
    @Override
    public void destroy() throws Exception {
        try {
            log.info("Closing elasticSearch client");
            if (client != null) {
                client.close();
            }
        } catch (final Exception e) {
            log.error("Error closing ElasticSearch client: ", e);
        }
    }
 
    @Override
    public RestHighLevelClient getObject() throws Exception {
        return restHighLevelClient;
    }
 
    @Override
    public Class<RestHighLevelClient> getObjectType() {
        return RestHighLevelClient.class;
    }
 
    @Override
    public void afterPropertiesSet() throws Exception {
        initClient();
    }
}
