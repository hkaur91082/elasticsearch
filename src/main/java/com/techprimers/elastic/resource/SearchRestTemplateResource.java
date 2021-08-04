package com.techprimers.elastic.resource;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.techprimers.elastic.model.Customer;
import com.techprimers.elastic.service.AmElasticsearchTemplate;
@RestController
@RequestMapping("/rest/customer")
public class SearchRestTemplateResource {
	 @Autowired
	 AmElasticsearchTemplate amElasticsearchTemplate;
	@PostMapping("/createCustomer")
    public String createProduct(@RequestBody Customer customer)  {
      System.out.println("inside ElasticsearchRestTemplate For customer");
      String iddata="";
      try{
    	  iddata=amElasticsearchTemplate.insert("customer", null, customer);
      System.out.println("id created is"+iddata);
      
      }catch(Exception e){
    	  System.out.println("error inserting customer index"+e.getMessage());
      }
      return iddata;
      }
	
	@PostMapping("/addCustomers/bulk")
    public boolean addItems(@RequestBody List<Customer> customerList) {
		System.out.println("inside ElasticsearchRestTemplate Bulk");
		boolean value=false;
		try{
		value= amElasticsearchTemplate.batchInsert("customer", null,customerList);
		}catch(IOException e){
			System.out.println("exception bulk customer");
		}
		return value;
	}
	@GetMapping("/getCustomers")
	  public  void viewCustomers(){
		System.out.println("GET MAPPING");
		List<Customer> list=amElasticsearchTemplate.findIndex("customer",Customer.class);
		//productService.findProductsByBrand(brandName);
		System.out.println("Number of customers"+list.size());
		list.forEach(customer->System.out.println(customer.toString()));
	}
	@GetMapping("/getSearchResults")
	public void getSearchQueryResults(){
		System.out.println("inside getsearchresults");
		try {
			amElasticsearchTemplate.search();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
