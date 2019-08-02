package net.postcore.aws;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class LambdaResponse<T> {

	private String body;
	private String statusCode = "200";
	private Map<String, String> headers = new HashMap<String, String>();
	
	public LambdaResponse() {
		super();
		this.headers.put("Content-Type", "application/json");
	}
	
	public LambdaResponse(T element) {
		this();
		Gson gson = new Gson();
		this.body = gson.toJson(element);
	}
	
	public LambdaResponse(String statusCode, String message) {
		this();
		this.statusCode = statusCode;
		this.body = message;
	}	
	
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public String getStatusCode() {
		return statusCode;
	}
	public void setStatusCode(String statusCode) {
		this.statusCode = statusCode;
	}
	public Map<String, String> getHeaders() {
		return headers;
	}
	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}	
}
