package com.techprimers.elastic.service;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
 
/**
 * AmElasticsearchTemplate
   * Use the API of 6.3.2, the following interfaces all support es of 6.1.1, if you add other interfaces, please test by yourself
 *
 * @author lxx
 * @date 2019/11/11
 */
@Service
@Slf4j
public class AmElasticsearchTemplate {
 
    @Resource
    private RestHighLevelClient restHighLevelClient;
 
    private static final String TYPE = "_doc";
 
    /**
           * Query the data of an index
     *
     * @date 2019/12/12
           * @param indexName index name
           * The data type returned by @param clazz
           * @return java.util.List <T> return list
     */
    public <T> List<T> findIndex(String indexName, Class<T> clazz){
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchResponse getResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits searchHits = getResponse.getHits();
            List<T> results = new ArrayList<>();
            for(SearchHit hit : searchHits){
                T t = JSON.parseObject(hit.getSourceAsString(),clazz);
                results.add(t);
            }
            return results;
        } catch (IOException e) {
            throw new RuntimeException("findIndex Exception",e);
        }
    }
 
    /**
           * Get indexed data based on id
     *
     * @date 2019/12/12
           * @param indexNames index names
     * @param id            id
           * The result type returned by @param clazz
           * @return T returns the result of the corresponding result type
     */
    public <T> T getById(String indexNames, String id,Class<T> clazz) {
         try {
             GetRequest request = new GetRequest(indexNames, TYPE, id);
             GetResponse getReponse = restHighLevelClient.get(request,RequestOptions.DEFAULT);
             if(getReponse.isExists()) {
                 String result = getReponse.getSourceAsString();
                 T t = JSONObject.parseObject(result,clazz);
                 return t;
             }
         }catch (Exception e){
            throw new RuntimeException("getById exception",e);
         }
         return null;
    }
 
    /**
           * Get data in batches according to ids, sort by id by default
     *
     * @date 2019/12/12
           * @param indexNames index names
           * @param clazz return type
           * @param ids id array
     * @return java.util.List<T>
     */
  /*  public <T> List<T>  getByIds(String indexNames, Class<T> clazz,String [] ids) {
         try {
             SearchRequest searchRequest = new SearchRequest(indexNames);
             SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
             QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(ids);
             sourceBuilder.query(queryBuilder);
             sourceBuilder.size(ids.length);
                           // id sort
             sourceBuilder.sort(new FieldSortBuilder("_id"));
             searchRequest.source(sourceBuilder);
             searchRequest.types(TYPE);
             SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
             SearchHits searchHits = searchResponse.getHits();
             List<T> results = new ArrayList<>();
             for(SearchHit hit : searchHits){
                 T t = JSON.parseObject(hit.getSourceAsString(),clazz);
                 results.add(t);
             }
             return results;
         }catch (Exception e){
             throw new RuntimeException("multiGetByIds exception",e);
         }
    }*/
 
    /**
           * Get data according to ids page
     *
     * @date 2019/12/12
           * @param indexNames index names
           * @param clazz type
           * @param ids id array
           * @param startIndex start index
           * @param pageSize how much data is displayed per page
     * @return java.util.List<T>
     */
    /*public <T> List<T>  getByIds(String indexNames, Class<T> clazz, String[] ids, int startIndex, int pageSize) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexNames);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(ids);
            sourceBuilder.query(queryBuilder);
            sourceBuilder.from(startIndex);
            sourceBuilder.size(pageSize);
                         // id sort
            sourceBuilder.sort(new FieldSortBuilder("_id"));
            searchRequest.source(sourceBuilder);
            searchRequest.types(TYPE);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            SearchHits searchHits = searchResponse.getHits();
            List<T> results = new ArrayList<>();
            for(SearchHit hit : searchHits){
                T t = JSON.parseObject(hit.getSourceAsString(),clazz);
                results.add(t);
            }
            return results;
        }catch (Exception e){
            throw new RuntimeException("multiGetByIds exception",e);
        }
    }*/
 
    /**
           * Sort paging
     *
     * @date 2019/12/12
           * @param indexNames index names
           * @param clazz type
           * @param sortName sort name
           * @param order
           * @param start start position
           * @param size Articles per page
     * @return java.util.List<T>
     */
    /*public <T> List<T> findPageBySort(String indexNames, Class<T> clazz,String sortName,SortOrder order, int start, int size) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexNames);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.from(start);
            sourceBuilder.size(size);
                         // rank sort
            sourceBuilder.sort(new FieldSortBuilder(sortName).order(order));
            searchRequest.source(sourceBuilder);
            searchRequest.types(TYPE);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            SearchHits searchHits = searchResponse.getHits();
            List<T> results = new ArrayList<>();
            for(SearchHit hit : searchHits){
                T t = JSON.parseObject(hit.getSourceAsString(),clazz);
                results.add(t);
            }
            return results;
        }catch (Exception e){
            throw new RuntimeException("multiGetByIds exception",e);
        }
    }*/
 
    /**
           * Search tab
     *
     * @date 2019/12/12
           * @param indexNames index names
           * @param pageStart page start position
           * @param pageSize shows the number of entries per page
           * @param sortBuilders sort Builder
           * @param queryBuilder Query Builder
           * @param clazz return type
     * @return java.util.List<T>
     */
    /*public <T> List<T>findPageSearch(String indexNames, Integer pageStart, Integer pageSize, List<SortBuilder> sortBuilders, QueryBuilder queryBuilder, Class<T> clazz) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexNames);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            if(pageStart != null) {
                sourceBuilder.from(pageStart);
                sourceBuilder.size(pageSize);
            }
            sourceBuilder.query(queryBuilder);
            if(sortBuilders != null) {
                                 // rank sort
                for (SortBuilder builder : sortBuilders) {
                    sourceBuilder.sort(builder);
                }
            }
            searchRequest.source(sourceBuilder);
            searchRequest.types(TYPE);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            SearchHits searchHits = searchResponse.getHits();
            List<T> results = new ArrayList<>();
            for(SearchHit hit : searchHits){
                T t = JSON.parseObject(hit.getSourceAsString(),clazz);
                results.add(t);
            }
            return results;
        }catch (Exception e){
            throw new RuntimeException("multiGetByIds exception",e);
        }
    }*/
 
    /**
           * Determine whether the index exists
     *
     * @date 2019/12/10
           * @param esIndex index name
     * @return boolean
     */
   /* public boolean existsIndex(String esIndex) {
        boolean exist = false;
        try {
            Response response = restHighLevelClient.getLowLevelClient().performRequest("HEAD", esIndex);
            exist = response.getStatusLine().getReasonPhrase().equals("OK");
        } catch (IOException e) {
            throw new RuntimeException("existsIndex exception",e);
        }
        return exist;
    }*/
 
    /**
           * Create index
     *
     * @date 2019/12/11
           * @param index index name
           * @param settings setting, can be empty
           * @param properties properties settings
     * @return boolean
     */
  /*  public boolean createIndex(String index,Map<String,Object> settings,Map<String,Map<String,String>> properties) throws IOException {
                 // Create an index request object and set the index name
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                 // Set index parameters
        Settings.Builder builder = Settings.builder();
        if(!CollectionUtils.isEmpty(settings)){
            for(Map.Entry<String,Object> entry : settings.entrySet()){
                if(entry.getValue() instanceof String){
                    builder.put(entry.getKey(),(String) entry.getValue());
                }else if(entry.getValue() instanceof Integer){
                    builder.put(entry.getKey(),(Integer) entry.getValue());
                }
            }
            createIndexRequest.settings(builder);
        }
        if(!CollectionUtils.isEmpty(properties)) {
            JSONObject sourceJson = new JSONObject();
            sourceJson.put("properties", JSONObject.toJSON(properties));
                         // Set the mapping
            createIndexRequest.mapping(TYPE, sourceJson.toJSONString(), XContentType.JSON);
        }
                 // Create an index operation client
        IndicesClient indices = restHighLevelClient.indices();
                 // Create response object
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);
                 // Get the response result
        boolean acknowledged = createIndexResponse.isAcknowledged();
        return acknowledged;
    }*/
 
    /**
           * Delete index
     *
     * @date 2019/12/11
           * @param indexName index name
     * @return boolean
     */
    public boolean deleteIndex(String indexName) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete(request,RequestOptions.DEFAULT);
        return deleteIndexResponse.isAcknowledged();
    }
 
    /**
           * Modify index settings
     *
     * @date 2019/12/11
           * @param indexName index name
           * @param settings index settings
     * @return boolean
     */
    /*public boolean putSetting(String indexName,Map<String,Object> settings) throws IOException {
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
        request.settings(settings);
        UpdateSettingsResponse updateSettingsResponse =
                restHighLevelClient.indices().putSettings(request);
        return updateSettingsResponse.isAcknowledged();
    }*/
 
    /**
           * Insert or update index data
     *
     * @date 2019/12/12
           * @param indexName index name
           * The name of @param id id
           * @param data inserted data
     * @return String returns the inserted or updated id
     */
    public <T> String insert(String indexName,String id,T data) throws IOException {
        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(data);
        System.out.println("inside customer insert()"+jsonObject.toJSONString());
        IndexRequest indexRequest = new IndexRequest(indexName, TYPE, jsonObject.getString(id))
                .source(jsonObject.toJSONString(),XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            return  indexResponse.getId();
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            return indexResponse.getId();
        }
        return null;
    }
    
    /**
           * Batch insert index data
     * 
     * @date 2019/12/11 
           * @param indexName index name
           * @param id id name of index
           * @param list inserted data
     * @return boolean
     */
    public <T>  boolean batchInsert(String indexName,String id,List<T> list) throws IOException {
        BulkRequest request = new BulkRequest();
                 // Null data directly returns success
        if(CollectionUtils.isEmpty(list)){
            return true;
        }
        for(T object : list){
            JSONObject jsonObject = (JSONObject) JSONObject.toJSON(object);
            request.add(new IndexRequest(indexName, TYPE, jsonObject.getString(id))
                    .source(jsonObject.toJSONString(),XContentType.JSON));
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(request,RequestOptions.DEFAULT);
        
                 // If one of the returned results is a failure
        if(bulkResponse != null && bulkResponse.hasFailures()){
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                                 // Because it is an add operation, only the added response is processed
                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                                 log.error ("Batch insert index failed: {}", failure);
                    }
                }
            }
            return false;
        }
        return true;
    }
   public void search() throws IOException {

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("customer");
        searchRequest.types("_doc");

        // Conditions=
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("city", "Beijing");
        TermQueryBuilder termQuery = QueryBuilders.termQuery("province", "Fujian");
        // Range query
        //RangeQueryBuilder timeFilter = QueryBuilders.rangeQuery("log_time").gt(12345).lt(343750);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        QueryBuilder totalFilter = QueryBuilders.boolQuery()
                .filter(matchQuery)
              //  .filter(timeFilter)
                .mustNot(termQuery);

        int size = 200;
        int from = 0;
       
       System.out.println("search results 1");
            try {
                sourceBuilder.query(totalFilter).from(from).size(size);
                sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
                searchRequest.source(sourceBuilder);

                SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    System.out.println(hit.getSourceAsString());
                }

               
            } catch (Exception e){
            	System.out.println("error while fetching"+e.getMessage());
            }
        }
}
