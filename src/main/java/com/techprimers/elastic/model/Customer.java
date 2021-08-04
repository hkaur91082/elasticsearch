package com.techprimers.elastic.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(indexName = "customer")
public class Customer {
	@Id
    private String id;
	
	@Field(type = FieldType.Text)
	private String name;
	
	@Field(type = FieldType.Text)
	private String city;
	
	@Field(type = FieldType.Text)
	private String useragent;
	
	@Field(type = FieldType.Text)
	private String sys_version;
	
	@Field(type = FieldType.Text)
	private String province;
	
	@Field(type = FieldType.Text)
	private String event_id;
	public String toString(){
		return "[name"+name+"city"+city+"id"+id+"province"+province+"useragent"+useragent+"]";
	}
}
