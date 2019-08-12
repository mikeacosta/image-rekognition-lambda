package net.postcore.aws;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ImageHandler implements RequestHandler<S3Event, LambdaResponse<List<String>>> {
   
	private Context context;
    private final AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
    		.withRegion("us-west-2").build();
    final AmazonSNS sns = AmazonSNSClient.builder().build();
    
    @Override
    public LambdaResponse<List<String>> handleRequest(S3Event event, Context cntxt) {
        context = cntxt;
    	context.getLogger().log("Received event: " + event.toJson());
        
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String image = event.getRecords().get(0).getS3().getObject().getKey();        

        S3Object s3Object = new S3Object().withName(image).withBucket(bucket);
        DetectLabelsRequest request = new DetectLabelsRequest()
        		.withImage(new Image().withS3Object(s3Object))
        		.withMinConfidence(90f);
        
    	List<String> results = new ArrayList<String>();
    	LambdaResponse<List<String>> response;
        
        try {
        	DetectLabelsResult labelsResult = rekognitionClient.detectLabels(request);
        	List<Label> labels = labelsResult.getLabels();
        	
        	context.getLogger().log("Detected " + labels.size() + " labels for " + image);
        	for (Label label : labels ) {
        		results.add(label.getName());
        		context.getLogger().log(label.getName() + " " + Float.toString(label.getConfidence()));
        	}
        } catch (AmazonRekognitionException e) {
        	context.getLogger().log("ERROR: " + e.getErrorMessage());
        	response = new LambdaResponse<List<String>>("500", e.getErrorMessage());
        }
        
        if (Integer.compare(results.size(), 0) > 0)
        	response = new LambdaResponse<List<String>>(results);
        else
        	response = new LambdaResponse<List<String>>("404", "Image not recognized.");
        
        publishSNS(results);
        return response;
    }
    
    private void publishSNS(List<String> labels) {   	
    	String arn = "arn:aws:sns:us-west-2:489967615225:image-rekognition";
    	String msg = new Gson().toJson(labels);
    	sns.publish(new PublishRequest(arn, msg));
    	context.getLogger().log(msg);
    }
}